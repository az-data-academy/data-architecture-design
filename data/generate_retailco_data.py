"""
generate_retailco_data.py — Générateur de données RetailCo
=============================================================
Génère ~1000 transactions en XOF (FCFA) — contexte UEMOA / Afrique de l'Ouest
TVA UEMOA : 18% (article 354 du CGI ivoirien et équivalents régionaux)

Anomalies incluses (à détecter en lab) :
  A1 — 12 doublons de transaction_id
  A2 — 18 lignes avec unit_price négatif ou nul
  A3 — 15 lignes avec discount_pct > 1.0 (impossible)
  A4 — 22 lignes avec total_amount incohérent (bug calcul TVA)
  A5 — 30 lignes avec customer_id NULL (anonymes)
  A6 — 8 lignes avec return_flag=True mais return_reason NULL
  A7 — 5 lignes avec quantity <= 0
  A8 — 10 outliers de prix (> 10× le prix normal de la catégorie)

Usage :
  python generate_retailco_data.py                    # 1000 lignes
  python generate_retailco_data.py --n 5000           # 5000 lignes
  python generate_retailco_data.py --no-anomalies     # données propres
  python generate_retailco_data.py --seed 99          # reproductible
"""

import argparse, csv, random, uuid
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker('fr_FR')

# TVA UEMOA / Côte d'Ivoire : 18%
TVA = 1.18
CURRENCY = 'XOF'

# ── Magasins — Afrique de l'Ouest (zone UEMOA) ───────────────────────────────
STORES = [
    ('STR-001', 'RetailCo Abidjan Plateau',        'Côte d\'Ivoire'),
    ('STR-002', 'RetailCo Abidjan Marcory',         'Côte d\'Ivoire'),
    ('STR-003', 'RetailCo Abidjan Cocody',          'Côte d\'Ivoire'),
    ('STR-004', 'RetailCo Abidjan Adjamé',          'Côte d\'Ivoire'),
    ('STR-005', 'RetailCo Dakar Plateau',           'Sénégal'),
    ('STR-006', 'RetailCo Dakar Almadies',          'Sénégal'),
    ('STR-007', 'RetailCo Dakar Mermoz',            'Sénégal'),
    ('STR-008', 'RetailCo Lomé Tokoin',             'Togo'),
    ('STR-009', 'RetailCo Lomé Bè',                 'Togo'),
    ('STR-010', 'RetailCo Cotonou Cadjehoun',       'Bénin'),
    ('STR-011', 'RetailCo Cotonou Ganhi',           'Bénin'),
    ('STR-012', 'RetailCo Bamako ACI 2000',         'Mali'),
    ('STR-013', 'RetailCo Ouagadougou Ouaga 2000',  'Burkina Faso'),
    ('STR-014', 'RetailCo Niamey Plateau',          'Niger'),
    ('STR-015', 'RetailCo Conakry Kaloum',          'Guinée'),
]

# ── Produits — Prix en XOF (FCFA) ────────────────────────────────────────────
PRODUCTS = [
    # (product_id, product_name, category, sub_category, base_price_xof)
    ('PRD-001', 'TV 55p 4K OLED',           'Électronique',  'Télévision',    395_000),
    ('PRD-002', 'TV 65p 8K',                'Électronique',  'Télévision',    650_000),
    ('PRD-003', 'Smartphone A52',           'Électronique',  'Mobile',        229_000),
    ('PRD-004', 'Smartphone Pro Max',       'Électronique',  'Mobile',        655_000),
    ('PRD-005', 'Casque BT Premium',        'Électronique',  'Audio',         130_000),
    ('PRD-006', 'Enceinte BT',              'Électronique',  'Audio',          58_000),
    ('PRD-007', 'Tablette 10p',             'Électronique',  'Tablette',      196_000),
    ('PRD-008', 'Laptop 15p',               'Électronique',  'Informatique',  524_000),
    ('PRD-009', 'Jeans slim H',             'Vêtements',     'Pantalon',       32_000),
    ('PRD-010', 'Jeans slim F',             'Vêtements',     'Pantalon',       32_000),
    ('PRD-011', 'Robe wax',                 'Vêtements',     'Robe',           28_000),
    ('PRD-012', 'Veste cuir',               'Vêtements',     'Veste',          85_000),
    ('PRD-013', 'Sneakers H',               'Vêtements',     'Chaussures',     52_000),
    ('PRD-014', 'Sneakers F',               'Vêtements',     'Chaussures',     52_000),
    ('PRD-015', 'Marmite alu 30L',          'Maison',        'Cuisine',        18_000),
    ('PRD-016', 'Cafetière expresso',       'Maison',        'Cuisine',        58_000),
    ('PRD-017', 'Mixeur cuisine',           'Maison',        'Cuisine',        95_000),
    ('PRD-018', 'Climatiseur 1.5 CV',       'Maison',        'Climatisation', 295_000),
    ('PRD-019', 'Canapé 3 places',          'Maison',        'Mobilier',      390_000),
    ('PRD-020', 'Tapis salon',              'Maison',        'Décoration',     75_000),
    ('PRD-021', 'Vélo électrique',          'Sport',         'Cyclisme',      558_000),
    ('PRD-022', 'Tapis de course',          'Sport',         'Fitness',       360_000),
    ('PRD-023', 'Raquette tennis',          'Sport',         'Tennis',         58_000),
    ('PRD-024', 'Chaussures running',       'Sport',         'Running',        65_000),
    ('PRD-025', 'Sac à dos randonnée',      'Sport',         'Outdoor',        52_000),
    ('PRD-026', 'Roman africain bestseller','Livres',        'Fiction',        12_000),
    ('PRD-027', 'Guide voyages Afrique',    'Livres',        'Voyage',         15_000),
    ('PRD-028', 'Bande dessinée',           'Livres',        'BD',              6_500),
    ('PRD-029', 'Parfum femme 50ml',        'Beauté',        'Parfum',         52_000),
    ('PRD-030', 'Crème visage karité',      'Beauté',        'Soin',           18_000),
]

PAYMENT_METHODS = ['Carte bancaire', 'Espèces', 'Orange Money', 'MTN Money',
                   'Wave', 'Virement', 'Moov Money', 'Chèque']
CHANNELS        = ['Magasin', 'Web', 'App mobile', 'Click & Collect', 'Téléphone']
RETURN_REASONS  = [
    'Défaut produit', 'Taille incorrecte', 'Changement d\'avis',
    'Produit non conforme', 'Délai livraison dépassé', 'Doublon commande',
]


def make_transaction(tx_id, store, product, customer_id, ts, anomalies):
    store_id, store_name, store_country = store
    prod_id, prod_name, category, sub_cat, base_price = product

    unit_price = round(base_price * random.uniform(0.88, 1.12) / 100) * 100  # arrondi 100 XOF
    quantity   = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]
    discount   = random.choices([0.0, 0.05, 0.10, 0.15, 0.20], weights=[50, 20, 15, 10, 5])[0]
    total      = round(quantity * unit_price * (1 - discount) * TVA / 100) * 100

    return_flag   = random.random() < 0.06
    return_reason = random.choice(RETURN_REASONS) if return_flag else None

    row = {
        'transaction_id':  tx_id,
        'store_id':        store_id,
        'store_name':      store_name,
        'store_country':   store_country,
        'customer_id':     customer_id,
        'product_id':      prod_id,
        'product_name':    prod_name,
        'category':        category,
        'sub_category':    sub_cat,
        'unit_price_xof':  unit_price,
        'quantity':        quantity,
        'discount_pct':    discount,
        'total_amount_xof': total,
        'currency':        CURRENCY,
        'payment_method':  random.choice(PAYMENT_METHODS),
        'channel':         random.choice(CHANNELS),
        'return_flag':     return_flag,
        'return_reason':   return_reason,
        'transaction_ts':  ts.isoformat(),
        'ingestion_ts':    (ts + timedelta(minutes=random.randint(1, 30))).isoformat(),
        '_anomaly':        '',
    }

    # Injection d'anomalies
    if anomalies.get('negative_price'):
        row['unit_price_xof']   = -abs(unit_price)
        row['total_amount_xof'] = -abs(total)
        row['_anomaly']         = 'A2_negative_price'
    elif anomalies.get('bad_discount'):
        row['discount_pct'] = round(random.uniform(1.1, 2.5), 3)
        row['_anomaly']     = 'A3_discount_over_100pct'
    elif anomalies.get('wrong_total'):
        # Bug : division au lieu de multiplication pour la TVA
        row['total_amount_xof'] = round(quantity * unit_price * (1 - discount) / TVA / 100) * 100
        row['_anomaly']         = 'A4_wrong_tva_calculation'
    elif anomalies.get('missing_return_reason'):
        row['return_flag']   = True
        row['return_reason'] = None
        row['_anomaly']      = 'A6_return_without_reason'
    elif anomalies.get('zero_quantity'):
        row['quantity']        = random.choice([0, -1, -2])
        row['total_amount_xof'] = 0
        row['_anomaly']        = 'A7_invalid_quantity'
    elif anomalies.get('price_outlier'):
        row['unit_price_xof']   = base_price * random.randint(15, 50)
        row['total_amount_xof'] = round(row['unit_price_xof'] * quantity * (1 - discount) * TVA / 100) * 100
        row['_anomaly']         = 'A8_price_outlier'

    return row


def generate(n=1000, seed=42, with_anomalies=True):
    random.seed(seed)
    Faker.seed(seed)
    customers = [f'cust-{i:05d}' for i in range(1, 801)]
    end_ts    = datetime(2026, 5, 31, 23, 59)
    start_ts  = end_ts - timedelta(days=365)

    def rand_ts():
        return start_ts + timedelta(seconds=random.randint(0, int((end_ts - start_ts).total_seconds())))

    rows = []
    n_normal = n - (120 if with_anomalies else 0)

    for _ in range(n_normal):
        rows.append(make_transaction(
            f'TXN-{uuid.uuid4().hex[:10].upper()}',
            random.choice(STORES), random.choice(PRODUCTS),
            random.choice(customers), rand_ts(), {}
        ))

    if not with_anomalies:
        return rows

    # A1 — 12 doublons
    for _ in range(12):
        dup = dict(random.choice(rows[:200]))
        dup['ingestion_ts'] = (datetime.fromisoformat(dup['ingestion_ts']) + timedelta(seconds=random.randint(5,120))).isoformat()
        dup['_anomaly'] = 'A1_duplicate_txn_id'
        rows.append(dup)

    # A2-A8
    anomaly_map = [
        (18, {'negative_price': True}),
        (15, {'bad_discount': True}),
        (22, {'wrong_total': True}),
        (8,  {'missing_return_reason': True}),
        (5,  {'zero_quantity': True}),
        (10, {'price_outlier': True}),
    ]
    for count, anom in anomaly_map:
        for _ in range(count):
            rows.append(make_transaction(
                f'TXN-{uuid.uuid4().hex[:10].upper()}',
                random.choice(STORES), random.choice(PRODUCTS),
                random.choice(customers), rand_ts(), anom
            ))

    # A5 — 30 anonymes
    for _ in range(30):
        rows.append(make_transaction(
            f'TXN-{uuid.uuid4().hex[:10].upper()}',
            random.choice(STORES), random.choice(PRODUCTS),
            None, rand_ts(), {}
        ))

    random.shuffle(rows)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--n',            type=int,  default=1000)
    parser.add_argument('--seed',         type=int,  default=42)
    parser.add_argument('--no-anomalies', action='store_true')
    parser.add_argument('--out',          type=str,  default=None)
    args = parser.parse_args()

    rows    = generate(n=args.n, seed=args.seed, with_anomalies=not args.no_anomalies)
    suffix  = '_clean' if args.no_anomalies else ''
    outfile = args.out or f'retailco_transactions{suffix}.csv'
    outpath = Path(__file__).parent / outfile

    fieldnames = ['transaction_id','store_id','store_name','store_country',
                  'customer_id','product_id','product_name','category','sub_category',
                  'unit_price_xof','quantity','discount_pct','total_amount_xof','currency',
                  'payment_method','channel','return_flag','return_reason',
                  'transaction_ts','ingestion_ts','_anomaly']

    with open(outpath, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    anomaly_counts = {}
    for r in rows:
        a = r.get('_anomaly','')
        if a:
            anomaly_counts[a] = anomaly_counts.get(a,0) + 1

    print(f'✅ {len(rows)} transactions → {outpath}')
    print(f'   Devise  : XOF (FCFA) — TVA UEMOA 18%')
    print(f'   Période : juin 2025 → mai 2026')
    print(f'   Pays    : CI, SN, TG, BJ, ML, BF, NE, GN')
    print(f'   Magasins: {len(STORES)} | Produits: {len(PRODUCTS)} | Clients: 800')
    if anomaly_counts:
        print(f'\n   Anomalies injectées ({sum(anomaly_counts.values())} lignes) :')
        labels = {
            'A1_duplicate_txn_id':      'A1 — Doublons transaction_id',
            'A2_negative_price':        'A2 — Prix négatifs',
            'A3_discount_over_100pct':  'A3 — Remise > 100%',
            'A4_wrong_tva_calculation': 'A4 — Bug TVA (÷1.18 au lieu de ×1.18)',
            'A6_return_without_reason': 'A6 — Retour sans raison',
            'A7_invalid_quantity':      'A7 — Quantité ≤ 0',
            'A8_price_outlier':         'A8 — Prix outlier (×15 à ×50)',
        }
        for code, count in sorted(anomaly_counts.items()):
            print(f'   {labels.get(code, code)}: {count}')
        anon = sum(1 for r in rows if not r['customer_id'] and not r['_anomaly'])
        print(f'   A5 — Clients anonymes (customer_id NULL): {anon}')


if __name__ == '__main__':
    main()
