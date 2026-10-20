import io
from pathlib import Path

import fitz  # PyMuPDF
import pytesseract
from PIL import Image

from . import events, store
from .config import CORPUS_DIR

OCR_LANG = "rus+eng"
MIN_TEXT_LEN = 30


def _ocr_image(img: Image.Image) -> str:
    try:
        return pytesseract.image_to_string(img, lang=OCR_LANG).strip()
    except Exception:
        return ""


def _ocr_page(page: fitz.Page) -> str:
    try:
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        return _ocr_image(img)
    except Exception:
        return ""


def _ocr_embedded_images(doc: fitz.Document, page: fitz.Page) -> str:
    texts = []
    for img_info in page.get_images(full=True):
        xref = img_info[0]
        try:
            data = doc.extract_image(xref)["image"]
            img = Image.open(io.BytesIO(data))
            if img.width < 50 or img.height < 50:
                continue
            text = _ocr_image(img)
            if text:
                texts.append(text)
        except Exception:
            continue
    return "\n".join(texts)


def extract_pdf(pdf_path: Path, session_id: str) -> int:
    """Извлекает текст из PDF в корпус (md-файлы + SQLite). Возвращает число страниц."""
    source = pdf_path.name
    doc = fitz.open(pdf_path)
    total = doc.page_count
    corpus_dir = CORPUS_DIR / session_id
    corpus_dir.mkdir(parents=True, exist_ok=True)

    for i, page in enumerate(doc):
        page_no = i + 1
        events.emit(session_id, "ingest", "running",
                    f"{source}: обработка страницы {page_no}/{total}",
                    progress=(i / total) * 100)

        text = (page.get_text() or "").strip()
        img_text = _ocr_embedded_images(doc, page)
        if img_text:
            text = (text + "\n\n" + img_text).strip() if text else img_text
        if len(text) < MIN_TEXT_LEN:
            ocr_text = _ocr_page(page)
            if ocr_text:
                text = (text + "\n\n" + ocr_text).strip() if text else ocr_text

        md = f"# {source} — стр. {page_no}\n\n{text}\n"
        md_path = corpus_dir / f"{pdf_path.stem}_p{page_no:03d}.md"
        md_path.write_text(md, encoding="utf-8")
        store.save_corpus_doc(session_id, source, page_no, text)

    doc.close()
    events.emit(session_id, "ingest", "running",
                f"{source}: готово ({total} стр.)", progress=100)
    return total
