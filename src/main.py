import pandas as pd
from dateutil import parser
from dateutil.relativedelta import relativedelta
from unidecode import unidecode


def parse_flex(date_str):
    try:
        return parser.parse(date_str)
    except:
        return pd.NaT


def citeste_csv(path):
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        print(f"Eroare la citirea fișierului: {e}")
        return None


def normalizeaza_data(df, coloana='DataCitire'):
    df[coloana] = df[coloana].astype(str).str.strip()
    date_parsed = pd.to_datetime(df[coloana], errors='coerce', dayfirst=True)
    mask_nat = date_parsed.isna()
    if mask_nat.any():
        fallback = df.loc[mask_nat, coloana].apply(parse_flex)
        date_parsed.loc[mask_nat] = fallback
    df[coloana] = date_parsed
    df['Locatie'] = df['Locatie'].apply(unidecode)
    return df


def convertire_consum_si_client_to_float(df, coloana='Consum_kWh'):
    df[coloana] = pd.to_numeric(df[coloana], errors='coerce')
    df['ClientID'] = pd.to_numeric(df['ClientID'], errors='coerce')
    df['ClientID'] = df['ClientID'].apply(
        lambda x: str(int(x)) if pd.notna(x) else pd.NA)
    return df


def raporteaza_consum_invalid(df, coloana='Consum_kWh'):
    invalide = df[df[coloana].isna()]
    if not invalide.empty:
        print("\n Randuri cu valori invalide in consum:\n")
        print(invalide[['ClientID', coloana]])
    else:
        print("\n Nu exista valori invalide in consum")
    return invalide


def elimina_valori_lipsa(df):
    df_curat = df.dropna(subset=['ClientID'])
    df_curat = df_curat.dropna(subset=['Consum_kWh'])
    df_curat = df_curat.dropna(subset=['DataCitire'])
    df_curat = df_curat.dropna(subset=['Locatie'])
    df_curat = df_curat.dropna(subset=['StatusContor'])
    df_curat = df_curat.dropna(subset=['TipClient'])
    return df_curat


def uniformizare_orase_si_statut(df, coras='Locatie', cstatut='StatusContor'):
    df[coras] = df[coras].str.strip().str.title()
    df[cstatut] = df[cstatut].str.strip().str.title()
    df['TipClient'] = df['TipClient'].str.strip().str.title()
    return df


def calculeaza_consum_mediu_pe_client(df):
    df['DataCitire'] = pd.to_datetime(
        df['DataCitire'], format='%d/%m/%Y', errors='coerce')
    df['DataCitireVeche'] = df['DataCitire'].apply(
        lambda x: x - relativedelta(months=1) if pd.notna(x) else pd.NaT)
    df['NumarZile'] = (df['DataCitire'] - df['DataCitireVeche']).dt.days
    df['ConsumMediuZi_kWh'] = df['Consum_kWh'] / df['NumarZile']
    return df


def main():
    path = r"c:\Users\andro\OneDrive\Desktop\Prima Apl\my-python-project\src\consum_energie_brut.csv"
    df = citeste_csv(path)
    if df is None:
        return
    df = normalizeaza_data(df, 'DataCitire')
    df = convertire_consum_si_client_to_float(df, 'Consum_kWh')
   # raporteaza_consum_invalid(df, 'Consum_kWh')
    df = elimina_valori_lipsa(df)
    df = uniformizare_orase_si_statut(df, 'Locatie', 'StatusContor')
    df['ConsumMediuZi_kWh'] = 0
    df = calculeaza_consum_mediu_pe_client(df)
    df = df.drop(columns=['DataCitireVeche', 'NumarZile'])
    df_finalizat = df
    df_finalizat = df_finalizat.sort_values(
        by=['TipClient', 'Consum_kWh'], ascending=[True, False])
    df_finalizat.to_csv(
        r"c:\Users\andro\OneDrive\Desktop\Prima Apl\my-python-project\src\consum_energie_finalizat.csv", index=False)
    # print(df[0:60])
    coloane_nevoie = ['ClientID', 'TipClient', 'Consum_kWh', 'DataCitire']
    df_dis = df[coloane_nevoie]
    df_dis.to_csv(
        r"c:\Users\andro\OneDrive\Desktop\Prima Apl\my-python-project\src\consum_energie_selectat.csv", index=False)

    df_dis = df_dis.sort_values(
        by=['TipClient', 'Consum_kWh'], ascending=[True, False])

    # print(df_dis[0:100])

    df_casnic = df_dis[df_dis['TipClient'] == 'Casnic']
    df_industrial = df_dis[df_dis['TipClient'] == 'Industrial']

    df_casnic.to_csv(
        r"c:\Users\andro\OneDrive\Desktop\Prima Apl\my-python-project\src\clienti_casnici.csv", index=False)
    df_industrial.to_csv(
        r"c:\Users\andro\OneDrive\Desktop\Prima Apl\my-python-project\src\clienti_industriali.csv", index=False)
    print(df_casnic[0:1])
    print(df_industrial[0:1])


if __name__ == "__main__":
    main()
