import io
import json
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st


# --------------------------------------------------
# App config
# --------------------------------------------------
st.set_page_config(
    page_title="Docling MVP Batch Uploader",
    page_icon="📄",
    layout="wide"
)

BASE_DIR = Path(".")
UPLOAD_DIR = BASE_DIR / "uploads"
OUTPUT_DIR = BASE_DIR / "outputs"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# UI header
# --------------------------------------------------
st.title("📄 Docling MVP - Batch Document & Table Extraction")
st.caption("Upload PDF, DOCX, or XLSX files and extract text + tables in one place.")

with st.container():
    st.markdown(
        """
        **This MVP does:**
        - PDF / DOCX via Docling
        - XLSX via pandas fallback
        - table previews
        - CSV / HTML / Markdown / JSON exports
        - ZIP download of all generated outputs

        **Cloud-safe mode**
        - Docling is loaded only when needed
        - OCR is disabled for PDFs
        - best for text-based PDFs
        - custom artifacts path is not used
        """
    )


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def safe_filename(name: str) -> str:
    keepchars = (" ", ".", "_", "-")
    cleaned = "".join(c for c in name if c.isalnum() or c in keepchars).strip()
    return cleaned or "uploaded_file"


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def save_uploaded_file(uploaded_file) -> Path:
    filename = f"{now_stamp()}_{safe_filename(uploaded_file.name)}"
    file_path = UPLOAD_DIR / filename
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path


def write_text_file(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def write_json_file(path: Path, data: Any) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8"
    )


def build_zip(file_paths: list[Path]) -> io.BytesIO:
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path in file_paths:
            if file_path.exists() and file_path.is_file():
                zf.write(file_path, arcname=file_path.name)
    zip_buffer.seek(0)
    return zip_buffer


def get_job_output_dir(file_path: Path) -> Path:
    job_dir = OUTPUT_DIR / file_path.stem
    job_dir.mkdir(parents=True, exist_ok=True)
    return job_dir


@st.cache_resource
def get_converter():
    """
    Lazy-load Docling only when needed.
    OCR is disabled for Streamlit Cloud stability.
    Intentionally do NOT set a custom artifacts_path here,
    because an incomplete local artifacts folder can trigger:
    'Missing safe tensors file: docling_artifacts/model.safetensors'
    """
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    pdf_options = PdfPipelineOptions()
    pdf_options.do_ocr = False

    return DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options)
        }
    )


def check_docling_available() -> tuple[bool, str]:
    """
    Try importing Docling without crashing the whole app.
    """
    try:
        from docling.document_converter import DocumentConverter  # noqa: F401
        return True, ""
    except Exception as e:
        return False, str(e)


# --------------------------------------------------
# XLSX fallback
# --------------------------------------------------
def process_xlsx_with_pandas(file_path: Path, job_output_dir: Path) -> dict:
    result = {
        "file_name": file_path.name,
        "file_type": "xlsx",
        "mode": "xlsx_pandas_fallback",
        "status": "success",
        "preview_text": "",
        "tables": [],
        "output_files": [],
        "errors": [],
    }

    try:
        excel_file = pd.ExcelFile(file_path)
        preview_lines = [f"# Spreadsheet: {file_path.name}", ""]
        preview_lines.append(f"Sheets detected: {', '.join(excel_file.sheet_names)}")
        preview_lines.append("")

        workbook_summary = {
            "file": file_path.name,
            "mode": result["mode"],
            "sheet_count": len(excel_file.sheet_names),
            "sheets": [],
            "errors": [],
        }

        for idx, sheet_name in enumerate(excel_file.sheet_names, start=1):
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)

                csv_path = job_output_dir / f"{file_path.stem}_sheet_{idx}_{safe_filename(sheet_name)}.csv"
                df.to_csv(csv_path, index=False)

                table_info = {
                    "index": idx,
                    "name": sheet_name,
                    "dataframe": df,
                    "csv_path": csv_path,
                    "html_path": None,
                    "markdown": f"### Sheet {idx}: {sheet_name}",
                    "rows": len(df),
                    "columns": len(df.columns),
                }

                result["tables"].append(table_info)
                result["output_files"].append(csv_path)

                workbook_summary["sheets"].append({
                    "name": sheet_name,
                    "rows": len(df),
                    "columns": list(df.columns),
                    "csv_file": csv_path.name,
                })

                preview_lines.append(f"## Sheet {idx}: {sheet_name}")
                preview_lines.append(f"Rows: {len(df)}")
                preview_lines.append(f"Columns: {len(df.columns)}")
                preview_lines.append("")

            except Exception as e:
                msg = f"Failed reading sheet '{sheet_name}': {e}"
                result["errors"].append(msg)
                workbook_summary["errors"].append(msg)

        preview_text = "\n".join(preview_lines)
        result["preview_text"] = preview_text

        md_path = job_output_dir / f"{file_path.stem}_summary.md"
        json_path = job_output_dir / f"{file_path.stem}_summary.json"

        write_text_file(md_path, preview_text)
        write_json_file(json_path, workbook_summary)

        result["output_files"].extend([md_path, json_path])

        if result["errors"] and not result["tables"]:
            result["status"] = "failed"
        elif result["errors"]:
            result["status"] = "partial_success"

    except Exception as e:
        result["status"] = "failed"
        result["errors"].append(f"XLSX fallback failed: {e}")

    return result


# --------------------------------------------------
# Docling processing
# --------------------------------------------------
def process_with_docling(file_path: Path, job_output_dir: Path, converter) -> dict:
    result = {
        "file_name": file_path.name,
        "file_type": file_path.suffix.lower().replace(".", ""),
        "mode": "docling",
        "status": "success",
        "preview_text": "",
        "tables": [],
        "output_files": [],
        "errors": [],
    }

    try:
        conv_result = converter.convert(str(file_path))
        doc = conv_result.document

        try:
            markdown_text = doc.export_to_markdown()
            result["preview_text"] = markdown_text
            md_path = job_output_dir / f"{file_path.stem}.md"
            write_text_file(md_path, markdown_text)
            result["output_files"].append(md_path)
        except Exception as e:
            result["errors"].append(f"Markdown export failed: {e}")

        try:
            if hasattr(doc, "export_to_dict"):
                doc_dict = doc.export_to_dict()
            elif hasattr(doc, "model_dump"):
                doc_dict = doc.model_dump()
            else:
                doc_dict = {"note": "No direct dictionary export method available in this Docling version."}

            json_path = job_output_dir / f"{file_path.stem}.json"
            write_json_file(json_path, doc_dict)
            result["output_files"].append(json_path)
        except Exception as e:
            result["errors"].append(f"JSON export failed: {e}")

        tables = getattr(doc, "tables", []) or []

        for idx, table in enumerate(tables, start=1):
            table_info = {
                "index": idx,
                "name": f"Table {idx}",
                "dataframe": None,
                "csv_path": None,
                "html_path": None,
                "markdown": None,
                "rows": None,
                "columns": None,
            }

            try:
                table_info["markdown"] = table.export_to_markdown()
            except Exception:
                table_info["markdown"] = f"### Table {idx}"

            try:
                html_path = job_output_dir / f"{file_path.stem}_table_{idx}.html"
                html_content = table.export_to_html()
                write_text_file(html_path, html_content)
                table_info["html_path"] = html_path
                result["output_files"].append(html_path)
            except Exception as e:
                result["errors"].append(f"HTML export failed for table {idx}: {e}")

            try:
                df = table.export_to_dataframe()
                table_info["dataframe"] = df
                table_info["rows"] = len(df)
                table_info["columns"] = len(df.columns)

                csv_path = job_output_dir / f"{file_path.stem}_table_{idx}.csv"
                df.to_csv(csv_path, index=False)
                table_info["csv_path"] = csv_path
                result["output_files"].append(csv_path)
            except Exception as e:
                result["errors"].append(f"CSV export failed for table {idx}: {e}")

            result["tables"].append(table_info)

        if result["errors"] and not result["preview_text"] and not result["tables"]:
            result["status"] = "failed"
        elif result["errors"]:
            result["status"] = "partial_success"

    except Exception as e:
        result["status"] = "failed"
        result["errors"].append(f"Docling processing failed: {e}")

    return result


def process_file(file_path: Path):
    job_output_dir = get_job_output_dir(file_path)
    suffix = file_path.suffix.lower()

    if suffix == ".xlsx":
        return process_xlsx_with_pandas(file_path, job_output_dir)

    ok, err = check_docling_available()
    if not ok:
        return {
            "file_name": file_path.name,
            "file_type": suffix.replace(".", ""),
            "mode": "docling",
            "status": "failed",
            "preview_text": "",
            "tables": [],
            "output_files": [],
            "errors": [f"Docling import failed before processing: {err}"],
        }

    try:
        converter = get_converter()
    except Exception as e:
        return {
            "file_name": file_path.name,
            "file_type": suffix.replace(".", ""),
            "mode": "docling",
            "status": "failed",
            "preview_text": "",
            "tables": [],
            "output_files": [],
            "errors": [f"Docling converter initialization failed: {e}"],
        }

    return process_with_docling(file_path, job_output_dir, converter)


# --------------------------------------------------
# Sidebar
# --------------------------------------------------
with st.sidebar:
    st.header("Options")
    show_full_preview = st.checkbox("Show full extracted preview", value=False)
    max_preview_chars = st.slider("Preview length", min_value=1000, max_value=15000, value=4000, step=500)
    st.markdown("---")
    st.info("Tip: Start with 3-5 real files from your own use case.")
    st.caption("PDF OCR is currently disabled for better Cloud compatibility.")

    docling_ok, docling_err = check_docling_available()
    if docling_ok:
        st.success("Docling import check passed")
    else:
        st.error("Docling import check failed")
        st.code(docling_err)


# --------------------------------------------------
# Upload
# --------------------------------------------------
uploaded_files = st.file_uploader(
    "Upload one or more files",
    type=["pdf", "docx", "xlsx"],
    accept_multiple_files=True,
    help="Supported: PDF, DOCX, XLSX"
)

process_clicked = st.button("🚀 Process uploaded files", type="primary", disabled=not uploaded_files)


# --------------------------------------------------
# Processing
# --------------------------------------------------
if process_clicked and uploaded_files:
    saved_paths = []
    all_results = []
    all_output_files = []

    progress_bar = st.progress(0, text="Preparing files...")
    status_placeholder = st.empty()

    total_files = len(uploaded_files)

    for file in uploaded_files:
        saved_paths.append(save_uploaded_file(file))

    for idx, file_path in enumerate(saved_paths, start=1):
        status_placeholder.info(f"Processing {idx}/{total_files}: {file_path.name}")

        file_result = process_file(file_path)
        all_results.append(file_result)
        all_output_files.extend(file_result["output_files"])

        progress_bar.progress(idx / total_files, text=f"Processed {idx}/{total_files} files")

    status_placeholder.success("Processing completed.")

    total_tables = sum(len(r["tables"]) for r in all_results)
    success_count = sum(1 for r in all_results if r["status"] == "success")
    partial_count = sum(1 for r in all_results if r["status"] == "partial_success")
    failed_count = sum(1 for r in all_results if r["status"] == "failed")

    st.markdown("## Results Overview")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Files processed", len(all_results))
    c2.metric("Tables found", total_tables)
    c3.metric("Successful", success_count)
    c4.metric("Partial / Failed", partial_count + failed_count)

    summary_tab, preview_tab, tables_tab, downloads_tab = st.tabs(
        ["Summary", "Preview", "Tables", "Downloads"]
    )

    with summary_tab:
        summary_rows = []
        for r in all_results:
            summary_rows.append({
                "File": r["file_name"],
                "Type": r["file_type"],
                "Mode": r["mode"],
                "Status": r["status"],
                "Tables": len(r["tables"]),
                "Errors": len(r["errors"]),
            })

        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

        for r in all_results:
            with st.expander(f"{r['file_name']} ({r['status']})"):
                if r["errors"]:
                    for err in r["errors"]:
                        st.warning(err)
                else:
                    st.success("No issues reported.")

    with preview_tab:
        for r in all_results:
            with st.expander(f"Preview - {r['file_name']}", expanded=False):
                preview = r["preview_text"] or "No preview available."
                if not show_full_preview and len(preview) > max_preview_chars:
                    preview = preview[:max_preview_chars] + "\n\n...[truncated]"

                st.text_area(
                    label=f"Extracted preview for {r['file_name']}",
                    value=preview,
                    height=280,
                    key=f"preview_{r['file_name']}"
                )

    with tables_tab:
        any_tables = any(r["tables"] for r in all_results)

        if not any_tables:
            st.info("No tables detected in the uploaded files.")
        else:
            for r in all_results:
                if not r["tables"]:
                    continue

                st.markdown(f"### {r['file_name']}")

                for table in r["tables"]:
                    table_title = table.get("name") or f"Table {table.get('index', '?')}"
                    with st.expander(table_title, expanded=False):
                        if table.get("markdown"):
                            st.markdown(table["markdown"])

                        df = table.get("dataframe")
                        if df is not None:
                            st.dataframe(df, use_container_width=True)

                        meta_cols = st.columns(2)
                        meta_cols[0].write(f"Rows: {table.get('rows', '-')}")
                        meta_cols[1].write(f"Columns: {table.get('columns', '-')}")

                        csv_path = table.get("csv_path")
                        if csv_path and Path(csv_path).exists():
                            with open(csv_path, "rb") as f:
                                st.download_button(
                                    label="Download CSV",
                                    data=f,
                                    file_name=Path(csv_path).name,
                                    mime="text/csv",
                                    key=f"csv_{r['file_name']}_{table['index']}"
                                )

                        html_path = table.get("html_path")
                        if html_path and Path(html_path).exists():
                            with open(html_path, "rb") as f:
                                st.download_button(
                                    label="Download HTML",
                                    data=f,
                                    file_name=Path(html_path).name,
                                    mime="text/html",
                                    key=f"html_{r['file_name']}_{table['index']}"
                                )

    with downloads_tab:
        st.subheader("Download everything")

        if all_output_files:
            zip_buffer = build_zip(all_output_files)
            st.download_button(
                label="📦 Download all generated outputs as ZIP",
                data=zip_buffer,
                file_name=f"docling_mvp_outputs_{now_stamp()}.zip",
                mime="application/zip"
            )
        else:
            st.info("No generated outputs available.")

        download_rows = []
        for r in all_results:
            for p in r["output_files"]:
                download_rows.append({
                    "Source file": r["file_name"],
                    "Generated file": Path(p).name,
                    "Path": str(p),
                })

        if download_rows:
            st.dataframe(pd.DataFrame(download_rows), use_container_width=True)

else:
    st.info("Upload one or more files and click **Process uploaded files**.")
