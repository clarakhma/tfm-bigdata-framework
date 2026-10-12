"""
TFM Framework — Lambda: GeneratePDF
State 13: GeneratePDF (runs after GenerateNarrative)
Builds the weekly executive PDF report:
  - Primary engine: WeasyPrint (HTML/CSS -> PDF), decision D20. Needs the WeasyPrint
    Lambda layer (see deploy/build_weasyprint_layer.sh).
  - Safety net: reportlab (pure Python, bundled in the function zip). If the WeasyPrint
    layer is not present the run still succeeds with a clean reportlab PDF.
  - Trend chart drawn as inline SVG (no matplotlib dependency).

Returns status + pdf_key (the S3 key) — the contract the State Machine's ResultSelector
expects. Universidad Europea de Madrid · Clara Khabbazi Manzur
"""

import boto3
import json
from datetime import datetime

s3 = boto3.client('s3', region_name='eu-west-1')


def lambda_handler(event, context):
    """
    Input (from Step Functions):
        event['sector']        — 'energia' or 'telecom'
        event['narrative']     — 5-section text from GenerateNarrative
        event['kpi_summary']   — dict from AggregateMetrics
        event['ml_results']    — dict from AggregateMetrics
        event['historico']     — list of last weeks (for the trend chart)
        event['alerts']        — list from AggregateMetrics
        event['reporting']     — reporting config (has output_bucket)
        event['runtime']['week']

    Output:
        {'status': 'ok', 'pdf_key': str, 'pdf_s3_path': str, 'pdf_size_kb': int, 'engine': str}
    """
    sector = event['sector']
    narrative = event.get('narrative', '')
    kpi_summary = event.get('kpi_summary', {})
    ml_results = event.get('ml_results', {})
    historico = event.get('historico', [])
    alerts = event.get('alerts', [])
    reporting = event.get('reporting', {})
    week = event['runtime']['week']

    # Output location — bucket + key from the reporting config
    raw = reporting.get('output_bucket', f's3://tfm-{sector}-ckm-ew1/reports/').replace('s3://', '').rstrip('/')
    bucket = raw.split('/')[0]
    base_prefix = '/'.join(raw.split('/')[1:]) or 'reports'
    pdf_key = f"{base_prefix}/{week}/report_{sector}_{week}.pdf"

    svg_chart = make_svg_chart(historico)
    html = build_html_report(sector, week, narrative, kpi_summary, ml_results, alerts, svg_chart)

    pdf_bytes, engine = render_pdf(html, sector, week, narrative, kpi_summary, ml_results, alerts, historico)

    s3.put_object(Bucket=bucket, Key=pdf_key, Body=pdf_bytes, ContentType='application/pdf')

    pdf_path = f"s3://{bucket}/{pdf_key}"
    size_kb = len(pdf_bytes) // 1024
    print(f"PDF generated ({engine}): {pdf_path} ({size_kb} KB)")
    return {'status': 'ok', 'pdf_key': pdf_key, 'pdf_s3_path': pdf_path,
            'pdf_size_kb': size_kb, 'engine': engine}


# ── Trend chart as inline SVG (no matplotlib) ─────────────────────────────────
def make_svg_chart(historico):
    """Simple line chart of weekly consumption from the historico list. Returns an SVG string."""
    pts = [(h.get('week', ''), h.get('total_consumption_kwh') or 0) for h in historico if h]
    if len(pts) < 2:
        return ""   # not enough points to draw a line

    w, h = 620, 200
    pad_l, pad_r, pad_t, pad_b = 50, 20, 20, 40
    xs = [p[1] for p in pts]
    vmin, vmax = min(xs), max(xs)
    span = (vmax - vmin) or 1
    n = len(pts)

    def x(i): return pad_l + i * (w - pad_l - pad_r) / (n - 1)
    def y(v): return pad_t + (1 - (v - vmin) / span) * (h - pad_t - pad_b)

    line = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, (_, v) in enumerate(pts))
    dots = "".join(f'<circle cx="{x(i):.1f}" cy="{y(v):.1f}" r="3" fill="#2563EB"/>' for i, (_, v) in enumerate(pts))
    labels = "".join(
        f'<text x="{x(i):.1f}" y="{h-14}" font-size="9" fill="#6B7280" text-anchor="middle">{wk[-3:]}</text>'
        for i, (wk, _) in enumerate(pts))
    vlabels = (f'<text x="8" y="{y(vmax):.1f}" font-size="9" fill="#9CA3AF">{vmax:.0f}</text>'
               f'<text x="8" y="{y(vmin):.1f}" font-size="9" fill="#9CA3AF">{vmin:.0f}</text>')

    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="100%">
      <rect x="0" y="0" width="{w}" height="{h}" fill="#F8FAFC" rx="6"/>
      <polyline points="{line}" fill="none" stroke="#2563EB" stroke-width="2"/>
      {dots}{labels}{vlabels}
    </svg>'''


# ── HTML report (WeasyPrint path) — English ───────────────────────────────────
def build_html_report(sector, week, narrative, kpi_summary, ml_results, alerts, svg_chart):
    sector_label = sector.title()   # etiqueta genérica (sin rama de sector)
    date_str = datetime.now().strftime("%d %B %Y")
    narrative_html = (narrative or "Narrative unavailable.").replace('\n', '<br/>')

    def kpi(label, value):
        return (f'<div class="kpi-box"><div class="label">{label}</div>'
                f'<div class="value">{value}</div></div>')

    kpis = "".join([
        kpi("Total consumption (kWh)", kpi_summary.get('total_consumption_kwh', '—')),
        kpi("Change vs last week", f"{kpi_summary.get('change_vs_last_week_pct', '—')}%"),
        kpi("Active alerts", kpi_summary.get('active_alerts', '—')),
    ])
    chart_block = f'<div class="section-title">Weekly trend</div><div class="chart">{svg_chart}</div>' if svg_chart else ""

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><style>
  @page {{ size: A4; margin: 2cm 2.2cm;
    @bottom-center {{ content: counter(page) " / " counter(pages); font-size: 9pt; color: #6B7280; }} }}
  body {{ font-family: 'Helvetica Neue', Arial, sans-serif; color: #111827; font-size: 10.5pt; line-height: 1.6; }}
  .header {{ background: #1E3A5F; color: white; padding: 20px 24px; border-radius: 8px; margin-bottom: 18px; }}
  .header h1 {{ margin: 0; font-size: 18pt; }}
  .header p {{ margin: 4px 0 0; font-size: 9.5pt; opacity: 0.85; }}
  .kpi-grid {{ display: flex; gap: 12px; margin-bottom: 8px; }}
  .kpi-box {{ flex: 1; background: #EFF6FF; border-left: 4px solid #2563EB; padding: 12px 14px; border-radius: 6px; }}
  .kpi-box .label {{ font-size: 8pt; color: #6B7280; text-transform: uppercase; letter-spacing: 0.05em; }}
  .kpi-box .value {{ font-size: 15pt; font-weight: bold; color: #1E3A5F; margin-top: 2px; }}
  .section-title {{ font-size: 11pt; font-weight: bold; color: #1E3A5F; border-bottom: 2px solid #DBEAFE;
                    padding-bottom: 4px; margin-top: 18px; margin-bottom: 10px; }}
  .narrative {{ font-size: 10pt; color: #374151; }}
  .chart {{ margin: 10px 0; }}
  .footer {{ margin-top: 26px; padding-top: 10px; border-top: 1px solid #E5E7EB; font-size: 8pt; color: #9CA3AF; text-align: center; }}
</style></head><body>
<div class="header">
  <h1>Weekly Executive Report — {sector_label}</h1>
  <p>Week: {week} &nbsp;|&nbsp; Generated: {date_str} &nbsp;|&nbsp; Model: {ml_results.get('best_model','—')} &nbsp;|&nbsp; TFM · Universidad Europea de Madrid</p>
</div>
<div class="kpi-grid">{kpis}</div>
{chart_block}
<div class="section-title">Executive analysis</div>
<div class="narrative">{narrative_html}</div>
<div class="footer">Automatically generated by the TFM Big Data Analytics framework · Narrative by Amazon Bedrock Nova Pro ·
Clara Khabbazi Manzur · Universidad Europea de Madrid</div>
</body></html>"""


# ── Render: WeasyPrint if available, else reportlab ───────────────────────────
def render_pdf(html, sector, week, narrative, kpi_summary, ml_results, alerts, historico):
    try:
        from weasyprint import HTML
        return HTML(string=html).write_pdf(), 'weasyprint'
    except Exception as e:
        print(f"WeasyPrint unavailable ({e}) — falling back to reportlab")
        return build_pdf_reportlab(sector, week, narrative, kpi_summary, ml_results, historico), 'reportlab'


def build_pdf_reportlab(sector, week, narrative, kpi_summary, ml_results, historico):
    """Pure-Python PDF (bundled reportlab). Always available — guarantees the run succeeds."""
    import io, re
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.lineplots import LinePlot

    NAVY = colors.HexColor('#1E3A5F'); GREY = colors.HexColor('#6B7280'); CREAM = colors.HexColor('#EFF6FF')
    st = getSampleStyleSheet()
    H1 = ParagraphStyle('H1', parent=st['Heading1'], fontSize=16, textColor=colors.white, spaceAfter=2)
    HSEC = ParagraphStyle('HSEC', parent=st['Heading2'], fontSize=12, textColor=NAVY, spaceBefore=10, spaceAfter=4)
    BODY = ParagraphStyle('BODY', parent=st['Normal'], fontSize=10, leading=15, textColor=colors.HexColor('#374151'), spaceAfter=4)
    SMALL = ParagraphStyle('SMALL', parent=st['Normal'], fontSize=8, textColor=GREY, alignment=1)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=1.6*cm, bottomMargin=1.4*cm, leftMargin=2*cm, rightMargin=2*cm)
    E = []
    label = sector.title()   # etiqueta genérica (sin rama de sector)
    date_str = datetime.now().strftime('%d %B %Y')
    head = Table([[Paragraph(f"Weekly Executive Report &mdash; {label}", H1)],
                  [Paragraph(f"Week: {week} &nbsp;|&nbsp; Generated: {date_str} &nbsp;|&nbsp; "
                             f"Model: {ml_results.get('best_model','—')} &nbsp;|&nbsp; TFM · Universidad Europea de Madrid",
                             ParagraphStyle('h2', parent=SMALL, textColor=colors.HexColor('#C7D2FE'), alignment=0))]],
                 colWidths=[17*cm])
    head.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),NAVY),('LEFTPADDING',(0,0),(-1,-1),12),
        ('TOPPADDING',(0,0),(0,0),10),('BOTTOMPADDING',(0,1),(0,1),10)]))
    E.append(head); E.append(Spacer(1,10))

    # Trend chart with reportlab.graphics
    pts = [(i, h.get('total_consumption_kwh') or 0) for i, h in enumerate(historico) if h]
    if len(pts) >= 2:
        d = Drawing(440, 130)
        lp = LinePlot(); lp.x = 40; lp.y = 15; lp.width = 380; lp.height = 100
        lp.data = [pts]; lp.lines[0].strokeColor = colors.HexColor('#2563EB'); lp.lines[0].strokeWidth = 2
        d.add(lp); E.append(Paragraph("Weekly trend", HSEC)); E.append(d); E.append(Spacer(1,6))

    patron = re.compile(r'^\d+\.\s+[A-Z][A-Z0-9 \-&/]{2,50}$')
    for linea in (narrative or '').split('\n'):
        t = linea.strip().replace('**', '')
        if not t:
            E.append(Spacer(1,3)); continue
        if t.startswith('#'):                              # encabezado markdown (###, ####) → título
            t = t.lstrip('#').strip().rstrip(':').strip()  # quita los '#' y el ':' final
            E.append(Paragraph(t, HSEC))
        elif patron.match(t) or (t.isupper() and len(t) < 55):
            E.append(Paragraph(t, HSEC))                   # "1. TITULO" o MAYÚSCULAS → título
        else:
            E.append(Paragraph(t, BODY))                   # resto → texto normal

    E.append(Spacer(1,16))
    E.append(Paragraph("Automatically generated by the TFM Big Data Analytics framework · "
                       "Narrative by Amazon Bedrock Nova Pro · Clara Khabbazi Manzur · Universidad Europea de Madrid", SMALL))
    doc.build(E)
    return buf.getvalue()
