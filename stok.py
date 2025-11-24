from flask import (
    Flask,
    request,
    render_template_string,
    send_file,
    redirect,
    url_for,
    flash,
)
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
from io import BytesIO, StringIO
import logging
import csv

app = Flask(__name__)
app.secret_key = "CHANGE_ME_TO_SOMETHING_RANDOM"  # flash mesajları için

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_decimal(text: str) -> Decimal:
    """Stok için güvenli decimal parse. Hatalıysa 0 döner."""
    if text is None:
        return Decimal(0)
    text = text.strip()
    if not text:
        return Decimal(0)
    try:
        return Decimal(text.replace(",", "."))
    except (InvalidOperation, ValueError):
        logger.warning("Sayısal olmayan stok değeri tespit edildi: %r, 0 kabul ediliyor", text)
        return Decimal(0)


def load_products(xml_bytes: bytes, key_field: str):
    """
    XML içinden ürünleri key_field'e göre mapler.
    key_field: 'barkod' veya 'stokKodu'
    Dönen:
        root: ET.Element
        products: { key: {"urun": element, "stok": Decimal} }
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        raise ValueError("Geçersiz XML formatı")

    if root.tag != "urunler":
        raise ValueError("Root elemanı 'urunler' olmalı")

    products = {}

    for urun in root.findall("urun"):
        key_el = urun.find(key_field)
        if key_el is None or key_el.text is None or not key_el.text.strip():
            # key olmayanları atlıyoruz
            continue

        key = key_el.text.strip()  # baş/son boşlukları temizle

        stok_el = urun.find("stok")
        stok = parse_decimal(stok_el.text if stok_el is not None else None)

        products[key] = {
            "urun": urun,
            "stok": stok,
        }

    return root, products


def merge_xml_feeds(store_xml: bytes, supplier_xml: bytes, key_field: str = "barkod"):
    """
    İki XML feed'ini birleştirir.
    - Mağaza XML'i referans alınır (ürün listesi ondan gelir).
    - Tedarikçideki stok aynı key'e göre bulunur.
    - stok_toplam = stok_magaza + stok_tedarikci
    - ÇIKTI: Mağaza XML'inin birebir yapısı, sadece <stok> alanları güncellenmiş.
    """
    if key_field not in {"barkod", "stokKodu"}:
        raise ValueError("key_field sadece 'barkod' veya 'stokKodu' olabilir")

    store_root, store_products = load_products(store_xml, key_field)
    _, supplier_products = load_products(supplier_xml, key_field)

    merged_rows = []  # UI / CSV için kullanılacak

    for key, sdata in store_products.items():
        urun_node = sdata["urun"]
        stok_magaza = sdata["stok"]

        if key in supplier_products:
            stok_tedarikci = supplier_products[key]["stok"]
        else:
            stok_tedarikci = Decimal(0)

        stok_toplam = stok_magaza + stok_tedarikci

        # Negatif olursa 0'a çek (teoride gerekmez ama tedbir)
        if stok_toplam < 0:
            stok_toplam = Decimal(0)

        # <stok> elementini bul ve toplam stok ile güncelle
        stok_el = urun_node.find("stok")
        if stok_el is None:
            stok_el = ET.SubElement(urun_node, "stok")

        # Tam sayı ise integer yaz, yoksa decimal string
        if stok_toplam == stok_toplam.to_integral_value():
            stok_el.text = str(int(stok_toplam))
        else:
            stok_el.text = format(stok_toplam, "f")

        # UI / CSV için satır ekle
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

    # Çıktı XML'ini bytes olarak üret
    merged_xml_bytes = ET.tostring(store_root, encoding="utf-8", xml_declaration=True)

    return merged_xml_bytes, merged_rows


INDEX_TEMPLATE = """
<!doctype html>
<html lang="tr">
<head>
  <meta charset="utf-8">
  <title>XML Stok Birleştirme Aracı</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Basit Bootstrap CDN -->
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    rel="stylesheet"
    integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH"
    crossorigin="anonymous"
  >
</head>
<body class="bg-light">
<div class="container py-5">
  <h1 class="mb-4">XML Stok Birleştirme Aracı</h1>

  {% with messages = get_flashed_messages() %}
    {% if messages %}
      <div class="alert alert-warning">
        {% for m in messages %}
          <div>{{ m }}</div>
        {% endfor %}
      </div>
    {% endif %}
  {% endwith %}

  <div class="card mb-4">
    <div class="card-body">
      <form method="post" action="{{ url_for('merge') }}" enctype="multipart/form-data">
        <div class="mb-3">
          <label class="form-label">Mağaza XML (senin stokların)</label>
          <input type="file" name="store_xml" class="form-control" accept=".xml" required>
        </div>
        <div class="mb-3">
          <label class="form-label">Tedarikçi XML</label>
          <input type="file" name="supplier_xml" class="form-control" accept=".xml" required>
        </div>
        <div class="mb-3">
          <label class="form-label">Eşleştirme anahtarı</label>
          <select name="key_field" class="form-select">
            <option value="barkod" selected>Barkod</option>
            <option value="stokKodu">Stok Kodu</option>
          </select>
        </div>
        <button type="submit" class="btn btn-primary">Stokları Birleştir</button>
      </form>
    </div>
  </div>

  {% if merged_rows %}
  <div class="card">
    <div class="card-header d-flex flex-wrap gap-2 justify-content-between align-items-center">
      <span>Birleştirilmiş Stok Özeti</span>
      <div class="d-flex gap-2">
        <a href="{{ url_for('download_merged') }}" class="btn btn-sm btn-success">
          Birleşik XML'i İndir
        </a>
        <a href="{{ url_for('download_merged_csv') }}" class="btn btn-sm btn-outline-secondary">
          CSV (Excel) Olarak İndir
        </a>
      </div>
    </div>
    <div class="card-body p-0">
      <div class="table-responsive">
        <table class="table table-sm table-striped mb-0">
          <thead class="table-light">
            <tr>
              <th>#</th>
              <th>Barkod / Key</th>
              <th>Ürün Adı</th>
              <th>Mağaza Stok</th>
              <th>Tedarikçi Stok</th>
              <th>Toplam Stok</th>
            </tr>
          </thead>
          <tbody>
            {% for row in merged_rows %}
            <tr>
              <td>{{ loop.index }}</td>
              <td>
                <div>{{ row.barkod }}</div>
                <div class="text-muted small">{{ row.key }}</div>
              </td>
              <td>{{ row.urunAdi }}</td>
              <td>{{ row.stok_magaza }}</td>
              <td>{{ row.stok_tedarikci }}</td>
              <td><strong>{{ row.stok_toplam }}</strong></td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </div>
  </div>
  {% endif %}
</div>
</body>
</html>
"""

# Bellekte tutulan son birleşik XML ve satırlar (demo için)
MERGED_XML_CACHE = {}


@app.route("/", methods=["GET"])
def index():
    merged_rows = MERGED_XML_CACHE.get("rows")
    return render_template_string(INDEX_TEMPLATE, merged_rows=merged_rows)


@app.route("/merge", methods=["POST"])
def merge():
    if "store_xml" not in request.files or "supplier_xml" not in request.files:
        flash("Lütfen hem mağaza hem tedarikçi XML dosyalarını seçin.")
        return redirect(url_for("index"))

    store_file = request.files["store_xml"]
    supplier_file = request.files["supplier_xml"]
    key_field = request.form.get("key_field", "barkod")

    if not store_file or not supplier_file:
        flash("Dosya yükleme sırasında hata oluştu.")
        return redirect(url_for("index"))

    try:
        store_bytes = store_file.read()
        supplier_bytes = supplier_file.read()
        merged_xml_bytes, merged_rows = merge_xml_feeds(
            store_xml=store_bytes,
            supplier_xml=supplier_bytes,
            key_field=key_field,
        )
    except ValueError as e:
        logger.exception("Birleştirme hatası")
        flash(str(e))
        return redirect(url_for("index"))
    except Exception as e:
        logger.exception("Beklenmeyen hata")
        flash(f"Beklenmeyen bir hata oluştu: {e}")
        return redirect(url_for("index"))

    # Belleğe yaz (demo için)
    MERGED_XML_CACHE["data"] = merged_xml_bytes
    MERGED_XML_CACHE["rows"] = merged_rows

    flash("Stoklar başarıyla birleştirildi.")
    return redirect(url_for("index"))


@app.route("/download-merged", methods=["GET"])
def download_merged():
    xml_bytes = MERGED_XML_CACHE.get("data")
    if not xml_bytes:
        flash("Önce stokları birleştirmeniz gerekiyor.")
        return redirect(url_for("index"))

    return send_file(
        BytesIO(xml_bytes),
        mimetype="application/xml",
        as_attachment=True,
        download_name="birlesik_stok.xml",
    )


@app.route("/download-merged-csv", methods=["GET"])
def download_merged_csv():
    rows = MERGED_XML_CACHE.get("rows")
    if not rows:
        flash("Önce stokları birleştirmeniz gerekiyor.")
        return redirect(url_for("index"))

    output = StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(["Barkod", "Key", "Ürün Adı", "Mağaza Stok", "Tedarikçi Stok", "Toplam Stok"])
    for r in rows:
        writer.writerow(
            [
                r["barkod"],
                r["key"],
                r["urunAdi"],
                r["stok_magaza"],
                r["stok_tedarikci"],
                r["stok_toplam"],
            ]
        )

    csv_bytes = output.getvalue().encode("utf-8-sig")  # Excel için BOM'lu UTF-8

    return send_file(
        BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name="birlesik_stok.csv",
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
