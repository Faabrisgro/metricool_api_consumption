# %%
import pandas as pd
import requests
import json
import re
from datetime import datetime
import os
from dotenv import load_dotenv

# %%
load_dotenv()

# %% [markdown]
# Lista de clientes para el requests.get | Clients we're requesting data with Metricool API KEY, this are params

# %% [markdown]
# Consumimos la API

# %%
r = requests.get(os.getenv('alpamanta_key'))


# %% [markdown]
# DATA WRANGLING - ALPAMANTA - 

# %%
text = r.text
json_data = json.loads(text)

# %%
df=pd.DataFrame(json_data)

# %%
df = pd.json_normalize(df.data)

# %%
df.columns

# %%
import pandas as pd

def iterate_json_normalize(df, col_name):
    """
    Iterates over the given column in a DataFrame and applies the pd.json_normalize method to each element.

    Parameters:
    df (pandas.DataFrame): The DataFrame to iterate over.
    col_name (str): The name of the column to apply pd.json_normalize to.

    Returns:
    pandas.DataFrame: The concatenated DataFrame of all the results from pd.json_normalize.
    """
    results = []
    for index in df.index:
        result = pd.json_normalize(df[col_name][index])
        results.append(result)

    return pd.concat(results, ignore_index=True)


# %%
mensajes_df = iterate_json_normalize(df,'messages')

# %%
participantes_df = iterate_json_normalize(df,'participants')

# %%
mensajes_df.rename(columns={'properties.reactions': 'reacciones'}, inplace= True)

# %%
mensajes_df = mensajes_df.fillna(0);

# %%
df_completo = pd.merge(mensajes_df, participantes_df, on='id', how='outer')

# %%
words_list = [
    'visitar',
    'conocer',
    'visit', 
    'visita', 
    'visitas', 
    'bodega', 
    'bodegas', 
    "disponibilidad", 
    "horario", 
    "horarios", 
    "instalaciones", 
    "experiencia", 
    'tour', 
    'degustaciones', 
    'degustación']

# %% [markdown]
# Primer día del mes

# %%
from datetime import datetime, timedelta

input_dt = datetime.today().date()
day_num = input_dt.strftime("%d")
first_day = input_dt - timedelta(days=int(day_num) - 1)
first_day

# %% [markdown]
# Último día del mes

# %%
next_month = input_dt.replace(day=28) + timedelta(days=4)
last_day = next_month - timedelta(days=next_month.day)
last_day

# %% [markdown]
# Último día del mes

# %%
start= first_day
end= last_day
start = pd.to_datetime(start)
end = pd.to_datetime(end)

# %%
def count_list_words_date(df, column_name, value_column_name, target_value, words_list, start_date, end_date):
    count = 0
    for index, row in df.iterrows():
        if row[value_column_name] != target_value:
            date = row['publicationDateTime']
            if pd.notnull(date):
                if start_date <= row['publicationDateTime'] <= end_date:
                    for word in words_list:
                        if word.lower() in str(row[column_name]).lower():
                            count += 1
                            break  # exit loop if a word is found 
    return count

# %%
df_completo['publicationDateTime'] = pd.to_datetime(df_completo['publicationDateTime'], errors='coerce', utc=True)
df_completo['publicationDateTime'] = df_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
df_completo['publicationDateTime'] = (df_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_alpamanta= count_list_words_date(df_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Errazuriz

# %%
r_ez = requests.get(os.getenv('errazuriz_key'))

# %%
r_ez_text = r_ez.text
ez_json = json.loads(r_ez_text)

# %%
ez_df = pd.DataFrame(ez_json)

# %%
ez_df = pd.json_normalize(ez_df.data)

# %%
ez_mensajes = iterate_json_normalize(ez_df,'messages')

# %%
ez_mensajes.shape

# %%
ez_mensajes = ez_mensajes.fillna(0)

# %%
ez_df_participantes = iterate_json_normalize(ez_df,'participants')

# %%
ez_completo = pd.merge(ez_mensajes, ez_df_participantes, on='id', how='outer')
ez_completo;

# %%
ez_completo['publicationDateTime'] = pd.to_datetime(ez_completo['publicationDateTime'], errors='coerce', utc=True)
ez_completo['publicationDateTime'] = ez_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
ez_completo['publicationDateTime'] = (ez_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_ez= count_list_words_date(ez_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Caliterra

# %%
r_cali = requests.get(os.getenv('caliterra_key'))

# %%
r_cali.status_code

# %%
r_cali_text = r_cali.text
cali_json = json.loads(r_cali_text)

# %%
cali_df = pd.DataFrame(cali_json)

# %%
cali_df = pd.json_normalize(cali_df.data)

# %%
cali_mensajes = iterate_json_normalize(cali_df,'messages')

# %%
cali_mensajes.shape

# %%
cali_participantes = iterate_json_normalize(cali_df,'participants')

# %%
cali_mensajes = cali_mensajes.fillna(0)

# %%
cali_participantes = iterate_json_normalize(cali_df,'participants')

# %%
cali_completo = pd.merge(cali_mensajes, cali_participantes, on='id', how='outer')
cali_completo;

# %%
cali_completo['publicationDateTime'] = pd.to_datetime(cali_completo['publicationDateTime'], errors='coerce', utc=True)
cali_completo['publicationDateTime'] = cali_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
cali_completo['publicationDateTime'] = (cali_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_cali = count_list_words_date(cali_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Arboleda

# %%
r_arbo = requests.get(os.getenv('arboleda_key'))

# %%
r_arbo.status_code

# %%
r_arbo_text = r_arbo.text
arbo_json = json.loads(r_arbo_text)

# %%
arbo_df = pd.DataFrame(arbo_json)

# %%
arbo_df = pd.json_normalize(arbo_df.data)

# %%
arbo_mensajes = iterate_json_normalize(arbo_df,'messages')

# %%
arbo_mensajes.shape

# %%
arbo_mensajes = arbo_mensajes.fillna(0)

# %%
arbo_participantes = iterate_json_normalize(arbo_df,'participants')

# %%
arbo_completo = pd.merge(arbo_mensajes, arbo_participantes, on='id', how='outer')
arbo_completo;

# %%
arbo_completo['publicationDateTime'] = pd.to_datetime(arbo_completo['publicationDateTime'], errors='coerce', utc=True)
arbo_completo['publicationDateTime'] = arbo_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
arbo_completo['publicationDateTime'] = (arbo_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_arbo= count_list_words_date(arbo_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Clos de los Siete 

# %%
r_c7 = requests.get(os.getenv('c7_key'))

# %%
r_c7.status_code

# %%
r_c7_text = r_c7.text
c7_json = json.loads(r_c7_text)

# %%
c7_df = pd.DataFrame(c7_json)

# %%
c7_df = pd.json_normalize(c7_df.data)

# %%
c7_mensajes = iterate_json_normalize(c7_df,'messages')

# %%
c7_mensajes.shape

# %%
c7_mensajes = c7_mensajes.fillna(0)

# %%
c7_participantes = iterate_json_normalize(c7_df,'participants')

# %%
c7_completo = pd.merge(c7_mensajes, c7_participantes, on='id', how='outer')
c7_completo;

# %%
c7_completo['publicationDateTime'] = pd.to_datetime(c7_completo['publicationDateTime'], errors='coerce', utc=True)
c7_completo['publicationDateTime'] = c7_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
c7_completo['publicationDateTime'] = (c7_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_c7 = count_list_words_date(c7_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Achaval Ferrer

# %%
r_achaval = requests.get(os.getenv('achaval_key'))

# %%
r_achaval.status_code

# %%
r_achaval_text = r_achaval.text
achaval_json = json.loads(r_achaval_text)

# %%
achaval_df = pd.DataFrame(achaval_json)

# %%
achaval_df = pd.json_normalize(achaval_df.data)

# %%
achaval_mensajes = iterate_json_normalize(achaval_df,'messages')

# %%
achaval_mensajes.shape

# %%
achaval_mensajes = achaval_mensajes.fillna(0)

# %%
achaval_participantes = iterate_json_normalize(achaval_df,'participants')

# %%
achaval_completo = pd.merge(achaval_mensajes, achaval_participantes, on='id', how='outer')
achaval_completo;

# %%
achaval_completo['publicationDateTime'] = pd.to_datetime(achaval_completo['publicationDateTime'], errors='coerce', utc=True)
achaval_completo['publicationDateTime'] = achaval_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
achaval_completo['publicationDateTime'] = (achaval_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_achaval = count_list_words_date(achaval_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Arínzano

# %%
r_arinzano = requests.get(os.getenv('arinzano_key'))

# %%
r_arinzano.status_code

# %%
r_arinzano_text = r_arinzano.text
arinzano_json = json.loads(r_arinzano_text)

# %%
arinzano_df = pd.DataFrame(arinzano_json)

# %%
arinzano_df = pd.json_normalize(arinzano_df.data)

# %%
arinzano_mensajes = iterate_json_normalize(arinzano_df,'messages')

# %%
arinzano_mensajes.shape

# %%
arinzano_mensajes = arinzano_mensajes.fillna(0)

# %%
arinzano_participantes = iterate_json_normalize(arinzano_df,'participants')

# %%
arinzano_completo = pd.merge(arinzano_mensajes, arinzano_participantes, on='id', how='outer')
arinzano_completo;

# %%
arinzano_completo['publicationDateTime'] = pd.to_datetime(arinzano_completo['publicationDateTime'], errors='coerce', utc=True)
arinzano_completo['publicationDateTime'] = arinzano_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
arinzano_completo['publicationDateTime'] = (arinzano_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_arinzano= count_list_words_date(arinzano_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Almaviva

# %%
r_almaviva = requests.get(os.getenv('almaviva_key'))

# %%
r_almaviva.status_code

# %%
r_almaviva_text = r_almaviva.text
almaviva_json = json.loads(r_almaviva_text)

# %%
almaviva_df = pd.DataFrame(almaviva_json)

# %%
almaviva_df = pd.json_normalize(almaviva_df.data)

# %%
almaviva_mensajes = iterate_json_normalize(almaviva_df,'messages')

# %%
almaviva_mensajes.shape

# %%
almaviva_mensajes = almaviva_mensajes.fillna(0)

# %%
almaviva_participantes = iterate_json_normalize(almaviva_df,'participants')

# %%
almaviva_completo = pd.merge(almaviva_mensajes, arinzano_participantes, on='id', how='outer')
almaviva_completo;

# %%
almaviva_completo['publicationDateTime'] = pd.to_datetime(almaviva_completo['publicationDateTime'], errors='coerce', utc=True)
almaviva_completo['publicationDateTime'] = almaviva_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
almaviva_completo['publicationDateTime'] = (almaviva_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_almaviva= count_list_words_date(almaviva_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Quimera

# %%
r_quimera = requests.get(os.getenv('quimera_key'))

# %%
r_quimera.status_code

# %%
r_quimera_text = r_quimera.text
quimera_json = json.loads(r_quimera_text)

# %%
quimera_df = pd.DataFrame(quimera_json)

# %%
quimera_df = pd.json_normalize(quimera_df.data)

# %%
quimera_mensajes = iterate_json_normalize(quimera_df,'messages')

# %%
quimera_mensajes.shape

# %%
quimera_mensajes = quimera_mensajes.fillna(0)

# %%
quimera_participantes = iterate_json_normalize(quimera_df,'participants')

# %%
quimera_completo = pd.merge(quimera_mensajes, quimera_participantes, on='id', how='outer')
quimera_completo;

# %%
quimera_completo['publicationDateTime'] = pd.to_datetime(quimera_completo['publicationDateTime'], errors='coerce', utc=True)
quimera_completo['publicationDateTime'] = quimera_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
quimera_completo['publicationDateTime'] = (quimera_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_quimera = count_list_words_date(quimera_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Mascota

# %%
r_mascota = requests.get(os.getenv('mascota_key'))

# %%
r_mascota.status_code

# %%
r_mascota_text = r_mascota.text
mascota_json = json.loads(r_mascota_text)

# %%
mascota_df = pd.DataFrame(mascota_json)

# %%
mascota_df = pd.json_normalize(mascota_df.data)

# %%
mascota_mensajes = iterate_json_normalize(mascota_df,'messages')

# %%
mascota_mensajes.shape

# %%
mascota_mensajes = mascota_mensajes.fillna(0)

# %%
mascota_participantes = iterate_json_normalize(mascota_df,'participants')

# %%
mascota_completo = pd.merge(mascota_mensajes, mascota_participantes, on='id', how='outer')
mascota_completo;

# %%
mascota_completo['publicationDateTime'] = pd.to_datetime(mascota_completo['publicationDateTime'], errors='coerce', utc=True)
mascota_completo['publicationDateTime'] = mascota_completo['publicationDateTime'].dt.date

# %%
from pandas.io.formats.format import Datetime64TZFormatter
mascota_completo['publicationDateTime'] = (mascota_completo['publicationDateTime']).astype('datetime64[ns]')

# %%
resultado_mascota = count_list_words_date(mascota_completo,'text', 'from',17841401836958650, words_list, start,end)

# %% [markdown]
# Dataframe Final

# %%
df = pd.DataFrame({
    'clientes': [],
    'total_conversaciones': [],
    'fecha': []
})
df

# %%
data = ['alpamanta', 'achaval ferrer', 'errazuriz', 'caliterra','closdelos7', 'arinzano', 'arboleda', 'almaviva', 'quimera', 'mascota']

resultados= [resultado_alpamanta, resultado_achaval, resultado_ez, resultado_cali, resultado_c7, resultado_arinzano, resultado_arbo, resultado_almaviva, resultado_quimera, resultado_mascota]

# %%
print(len(data), len(resultados))

# %%
df['total_conversaciones'] = resultados
df['clientes'] = data
df['fecha'] = datetime.today().strftime('%Y-%m-%d')

# %%
df

# %%
df.to_csv('api-metricool.csv', index=False)


