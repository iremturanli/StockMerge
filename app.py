import os
import logging
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode
import csv
from io import StringIO

import httpx
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# .env dosyasını oku
load_dotenv()

# ---------------------------------------------------
# Konfigürasyon
# ---------------------------------------------------
STORE_FEED_URL = os.getenv("STORE_FEED_URL")
SUPPLIER_FEED_URL = os.getenv("SUPPLIER_FEED_URL")
API_TOKEN = os.getenv("API_TOKEN", "change-me")

if not STORE_FEED_URL or not SUPPLIER_FEED_URL:
    raise RuntimeError("STORE_FEED_URL ve SUPPLIER_FEED_URL environment değişkenlerini tanımlamalısın.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="XML Ürün Birleştirme Servisi",
    description=(
        "Mağaza XML feed'i ile tedarikçi XML feed'ini birleştirip, "
        "stokları toplar ve tek bir XML feed olarak sunar."
    ),
    version="1.0.0",
)

templates = Jinja2Templates(directory="templates")

# İstersen ileride CSS/JS koyarsın diye static klasörünü de mount edelim
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------------------------------
# Yardımcı fonksiyonlar
# ---------------------------------------------------

def normalize_url_param(value: str | None) -> str | None:
    """URL query parametresini temizle."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def resolve_feed_urls(store_override: str | None, supplier_override: str | None) -> tuple[str, str]:
    """Override varsa onu, yoksa .env'deki feed URL'lerini döner."""
    store_url = store_override or STORE_FEED_URL
    supplier_url = supplier_override or SUPPLIER_FEED_URL

    missing = []
    if not store_url:
        missing.append("STORE_FEED_URL")
    if not supplier_url:
        missing.append("SUPPLIER_FEED_URL")
    if missing:
        raise ValueError(f"{', '.join(missing)} tanımlı değil")

    return store_url, supplier_url


def build_override_query_suffix(store_override: str | None, supplier_override: str | None) -> str:
    """UI linkleri için override parametrelerini query string olarak üret."""
    params = {}
    if store_override:
        params["store_url"] = store_override
    if supplier_override:
        params["supplier_url"] = supplier_override
    if not params:
        return ""
    return "&" + urlencode(params)


def parse_decimal(text: str) -> Decimal:
    """Stok alanlarını güvenli şekilde Decimal'e çevir. Boş veya hatalıysa 0 kabul edilir."""
    if text is None:
        return Decimal(0)
    text = text.strip()
    if not text:
        return Decimal(0)
    try:
        return Decimal(text.replace(",", "."))
    except (InvalidOperation, ValueError):
        logger.warning("Sayısal olmayan stok değeri tespit edildi: %r -> 0 kabul ediliyor", text)
        return Decimal(0)


def load_products(xml_bytes: bytes, key_field: str):
    """
    XML içinden ürünleri key_field (barkod veya stokKodu) üzerinden map'ler.

    Beklenen format (senin gönderdiğin):
    <urunler>
      <urun>
        <stokKodu>...</stokKodu>
        <stok>...</stok>
        <desi>...</desi>
        <barkod>...</barkod>
        <urunAdi>...</urunAdi>
        ...
      </urun>
      ...
    </urunler>
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.error("XML parse hatası: %s", e)
        raise ValueError("Geçersiz XML formatı")

    if root.tag != "urunler":
        raise ValueError("Root elemanı 'urunler' olmalı")

    products = {}

    for urun in root.findall("urun"):
        key_el = urun.find(key_field)
        if key_el is None or key_el.text is None or not key_el.text.strip():
            # Anahtar alanı olmayan ürünü atlıyoruz (barkodsuz ürün)
            continue

        key = key_el.text.strip()

        stok_el = urun.find("stok")
        stok = parse_decimal(stok_el.text if stok_el is not None else None)

        products[key] = {
            "urun": urun,  # XML node'un kendisi
            "stok": stok,
        }

    return root, products


def compute_merged(store_xml: bytes, supplier_xml: bytes, key_field: str = "barkod"):
    """
    İki XML feed'ini birleştirir ve hem birleşmiş XML'i hem de
    UI için ürün bazlı özet bilgileri döner.

    - Mağaza XML'i (store_xml) referans alınır:
      * Ürün listesi buradan gelir.
      * Fiyat alanları (urunFiyati, urunSiteFiyati, urunTrendyolFiyati, vs.) mağazadan alınır.
    - Tedarikçi XML'i (supplier_xml) sadece stok eklemek için kullanılır.
    - Eşleştirme default 'barkod' üzerinden yapılır.
    - Çıkan XML: mağaza XML'inin birebir yapısı, sadece <stok> toplam stok ile güncellenmiş olur.
    """
    if key_field not in {"barkod", "stokKodu"}:
        raise ValueError("key_field sadece 'barkod' veya 'stokKodu' olabilir")

    store_root, store_products = load_products(store_xml, key_field)
    _, supplier_products = load_products(supplier_xml, key_field)

    logger.info("Mağaza ürün sayısı: %d", len(store_products))
    logger.info("Tedarikçi ürün sayısı: %d", len(supplier_products))

    merged_rows = []  # UI tablosu için

    for key, sdata in store_products.items():
        urun_node = sdata["urun"]
        stok_magaza = sdata["stok"]

        if key in supplier_products:
            stok_tedarikci = supplier_products[key]["stok"]
        else:
            stok_tedarikci = Decimal(0)

        stok_toplam = stok_magaza + stok_tedarikci

        if stok_toplam < 0:
            logger.warning("Negatif stok hesaplandı (%s), 0'a çekiliyor", stok_toplam)
            stok_toplam = Decimal(0)

        # Mağaza XML'indeki <stok> elementini toplam stok ile güncelle
        stok_el = urun_node.find("stok")
        if stok_el is None:
            stok_el = ET.SubElement(urun_node, "stok")

        # Tam sayı ise integer yaz, yoksa decimal string
        if stok_toplam == stok_toplam.to_integral_value():
            stok_el.text = str(int(stok_toplam))
        else:
            stok_el.text = format(stok_toplam, "f")

        # UI özet satırı
        barkod_el = urun_node.find("barkod")
        urun_adi_el = urun_node.find("urunAdi")

        merged_rows.append(
            {
                "key": key,
                "barkod": barkod_el.text.strip() if barkod_el is not None and barkod_el.text else "",
                "urunAdi": urun_adi_el.text.strip() if urun_adi_el is not None and urun_adi_el.text else "",
                "stok_magaza": int(stok_magaza) if stok_magaza == stok_magaza.to_integral_value() else float(stok_magaza),
                "stok_tedarikci": int(stok_tedarikci) if stok_tedarikci == stok_tedarikci.to_integral_value() else float(stok_tedarikci),
                "stok_toplam": int(stok_toplam) if stok_toplam == stok_toplam.to_integral_value() else float(stok_toplam),
            }
        )

    merged_xml_bytes = ET.tostring(store_root, encoding="utf-8", xml_declaration=True)
    return merged_xml_bytes, merged_rows


async def fetch_xml(url: str) -> bytes:
    """Verilen URL'den XML içeriğini çeker."""
    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(url)
        except httpx.RequestError as e:
            logger.error("XML feed isteği başarısız: %s -> %s", url, e)
            raise HTTPException(status_code=502, detail=f"Upstream feed erişilemedi: {url}")
    if resp.status_code != 200:
        logger.error("XML feed HTTP %s döndü: %s", resp.status_code, url)
        raise HTTPException(status_code=502, detail=f"Upstream feed HTTP {resp.status_code}: {url}")

    return resp.content


# ---------------------------------------------------
# API endpoint'leri
# ---------------------------------------------------

@app.get("/", summary="Servis durumu")
async def root():
    return {
        "service": "XML Ürün Birleştirme Servisi",
        "status": "ok",
        "store_feed_url": STORE_FEED_URL,
        "supplier_feed_url": SUPPLIER_FEED_URL,
        "merged_endpoint_example": "/export/products-xml/merged?token=YOUR_TOKEN",
    }


# @app.get(
#     "/export/products-xml/merged",
#     summary="Birleştirilmiş XML ürün feed'i",
#     response_class=Response,
# )
# async def get_merged_products_xml(
#     token: str = Query(..., description="Basit güvenlik için API token"),
#     key_field: str = Query("barkod", regex="^(barkod|stokKodu)$", description="Ürün eşleştirme alanı"),
# ):
#     """
#     Mağaza ve tedarikçi XML feed'lerini anlık olarak çekip,
#     stokları birleştirerek tek bir XML döner.

#     - Token doğru olmalı (API_TOKEN).
#     - XML formatı, mağaza feed'inin formatı ile birebir aynıdır.
#     - Sadece <stok> alanları toplam stok ile güncellenir.
#     """
#     if token != API_TOKEN:
#         raise HTTPException(status_code=403, detail="Geçersiz token")

#     store_xml = await fetch_xml(STORE_FEED_URL)
#     supplier_xml = await fetch_xml(SUPPLIER_FEED_URL)

#     try:
#         merged_xml, _ = compute_merged(store_xml=store_xml, supplier_xml=supplier_xml, key_field=key_field)
#     except ValueError as e:
#         raise HTTPException(status_code=500, detail=str(e))
#     except Exception as e:
#         logger.exception("Merge sırasında beklenmeyen hata")
#         raise HTTPException(status_code=500, detail=f"Birleştirme sırasında hata: {e}")

#     return Response(content=merged_xml, media_type="application/xml")

@app.get(
    "/export/products-xml/merged",
    summary="Birleştirilmiş XML ürün feed'i",
    response_class=Response,
)
async def get_merged_products_xml(
    token: str = Query(..., description="Basit güvenlik için API token"),
    key_field: str = Query("barkod", regex="^(barkod|stokKodu)$", description="Ürün eşleştirme alanı"),
    download: bool = Query(False, description="True ise dosya indirme davranışı tetiklenir"),
    store_url: str | None = Query(None, description="Mağaza feed URL override"),
    supplier_url: str | None = Query(None, description="Tedarikçi feed URL override"),
):
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Geçersiz token")

    store_override = normalize_url_param(store_url)
    supplier_override = normalize_url_param(supplier_url)

    try:
        resolved_store_url, resolved_supplier_url = resolve_feed_urls(store_override, supplier_override)
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    store_xml = await fetch_xml(resolved_store_url)
    supplier_xml = await fetch_xml(resolved_supplier_url)

    try:
        merged_xml, _ = compute_merged(store_xml=store_xml, supplier_xml=supplier_xml, key_field=key_field)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.exception("Merge sırasında beklenmeyen hata")
        raise HTTPException(status_code=500, detail=f"Birleştirme sırasında hata: {e}")

    headers = {}
    if download:
        headers["Content-Disposition"] = 'attachment; filename="birlesik_stok.xml"'

    return Response(content=merged_xml, media_type="application/xml", headers=headers)


@app.get("/health", summary="Health check")
async def health():
    return JSONResponse({"status": "ok"})

#CSV endpoint
@app.get(
    "/export/products-xml/merged.csv",
    summary="Birleştirilmiş stokların CSV (Excel) çıktısı",
    response_class=Response,
)
async def get_merged_products_csv(
    token: str = Query(..., description="Basit güvenlik için API token"),
    key_field: str = Query("barkod", regex="^(barkod|stokKodu)$", description="Ürün eşleştirme alanı"),
    store_url: str | None = Query(None, description="Mağaza feed URL override"),
    supplier_url: str | None = Query(None, description="Tedarikçi feed URL override"),
):
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Geçersiz token")

    store_override = normalize_url_param(store_url)
    supplier_override = normalize_url_param(supplier_url)

    try:
        resolved_store_url, resolved_supplier_url = resolve_feed_urls(store_override, supplier_override)
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    # XML feed'leri çek
    store_xml = await fetch_xml(resolved_store_url)
    supplier_xml = await fetch_xml(resolved_supplier_url)

    # Merge + satırları çıkar
    try:
        _, merged_rows = compute_merged(
            store_xml=store_xml,
            supplier_xml=supplier_xml,
            key_field=key_field,
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.exception("CSV üretimi sırasında hata")
        raise HTTPException(status_code=500, detail=f"CSV üretimi sırasında hata: {e}")

    # CSV üret
    output = StringIO()
    # writer = csv.writer(output, delimiter=";")
    writer = csv.writer(output, delimiter=",", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["Barkod", "Key", "Ürün Adı", "Mağaza Stok", "Tedarikçi Stok", "Toplam Stok"])

    for r in merged_rows:
        writer.writerow([
            r["barkod"],
            r["key"],
            r["urunAdi"],
            r["stok_magaza"],
            r["stok_tedarikci"],
            r["stok_toplam"],
        ])

    csv_bytes = output.getvalue().encode("utf-8-sig")  # Excel için BOM

    headers = {
        "Content-Disposition": 'attachment; filename="birlesik_stok.csv"'
    }

    return Response(content=csv_bytes, media_type="text/csv", headers=headers)


# ---------------------------------------------------
# UI endpoint
# ---------------------------------------------------

@app.get("/ui", summary="Web arayüzü ile test", include_in_schema=False)
async def ui(
    request: Request,
    run: bool = Query(False, description="True ise anlık merge yap ve sonucu göster"),
    key_field: str = Query("barkod", regex="^(barkod|stokKodu)$"),
    store_url: str | None = Query(None, description="Mağaza feed URL override"),
    supplier_url: str | None = Query(None, description="Tedarikçi feed URL override"),
):
    """
    İnsan gözüyle test etmeye yarayan şık UI.
    - run=true ile çağrılırsa upstream feed'leri çekip tabloda gösterir.
    """
    merged_rows = None
    error = None
    store_override = normalize_url_param(store_url)
    supplier_override = normalize_url_param(supplier_url)
    override_suffix = build_override_query_suffix(store_override, supplier_override)
    store_url_display = store_override or STORE_FEED_URL or ""
    supplier_url_display = supplier_override or SUPPLIER_FEED_URL or ""

    if run:
        try:
            resolved_store_url, resolved_supplier_url = resolve_feed_urls(store_override, supplier_override)
            store_xml = await fetch_xml(resolved_store_url)
            supplier_xml = await fetch_xml(resolved_supplier_url)
            _, merged_rows = compute_merged(store_xml=store_xml, supplier_xml=supplier_xml, key_field=key_field)
        except ValueError as e:
            error = str(e)
        except HTTPException as e:
            error = f"Feed erişim hatası: {e.detail}"
        except Exception as e:
            logger.exception("UI üzerinden merge hatası")
            error = f"Birleştirme hatası: {e}"

    # UI'da çok uzun olmasın diye max 100 ürünü göster
    if merged_rows:
        total_count = len(merged_rows)
        merged_rows_preview = merged_rows[:100]
    else:
        total_count = 0
        merged_rows_preview = None

    return templates.TemplateResponse(
        "ui.html",
        {
            "request": request,
            "store_url": store_url_display,
            "supplier_url": supplier_url_display,
            "api_token": API_TOKEN,
            "key_field": key_field,
            "merged_rows": merged_rows_preview,
            "total_count": total_count,
            "error": error,
            "override_suffix": override_suffix,
        },
    )


# ---------------------------------------------------
# Lokal çalıştırma komutu:
# uvicorn app:app --host 0.0.0.0 --port 8000 --reload
# ---------------------------------------------------
