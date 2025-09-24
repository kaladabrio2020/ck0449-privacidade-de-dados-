# %% [markdown]
# ### Trabalho 1 : em alguém api?

# %%
# importando libs
import polars as pl
import duckdb

# %%
cand_2020 = pl.read_csv('dataset\\consulta_cand_2020_CE.csv')
cand_2020.head(2)

# %%
covid = pl.read_csv('dataset\\dados_covid-ce_trab02.csv')
covid.head(2)

# %%
## Selecionado colunas de interesse para candidatos.
 
### NM_CANDIDATO, NR_CPF_CANDIDATO.

### Semi-identificadores: NM_UE, DT_NASCIMENTO, DS_GENERO, DS_COR_RACA
cand_2020 = cand_2020.select([
    'NM_CANDIDATO', 'NR_CPF_CANDIDATO', 'NM_UE', 'DT_NASCIMENTO', 'DS_GENERO', 'DS_COR_RACA'
])

# %%
print("\nColunas selecionadas do dataset de candidatos:", cand_2020.columns)
print(covid.head(2))

# %%
## Selecionando colunas de interesse para covid.

### municipioCaso, bairroCaso, sexoCaso, dataNascimento, racaCor
### resultadoFinalExame + 14 comorbidades
### comorbidadePuerperaSivep	comorbidadeCardiovascularSivep	comorbidadeHematologiaSivep	comorbidadeSindromeDownSivep	comorbidadeHepaticaSivep	comorbidadeAsmaSivep	comorbidadeDiabetesSivep	comorbidadeNeurologiaSivep	comorbidadePneumopatiaSivep	comorbidadeImunodeficienciaSivep	comorbidadeRenalSivep	comorbidadeObesidadeSivep	comorbidadeHiv	comorbidadeNeoplasias
covid = covid.select([
    'municipioCaso', 'bairroCaso', 'sexoCaso', 'dataNascimento', 'racaCor', 'resultadoFinalExame',
    'comorbidadePuerperaSivep','comorbidadeCardiovascularSivep','comorbidadeHematologiaSivep',
    'comorbidadeSindromeDownSivep','comorbidadeHepaticaSivep','comorbidadeAsmaSivep',
    'comorbidadeDiabetesSivep','comorbidadeNeurologiaSivep','comorbidadePneumopatiaSivep',
    'comorbidadeImunodeficienciaSivep','comorbidadeRenalSivep','comorbidadeObesidadeSivep',
    'comorbidadeHiv','comorbidadeNeoplasias'
]) 

covid.head(2)

# %%
print("\nColunas selecionadas do dataset de covid:", covid.columns)
print(covid.head(2))

# %%
print('Fazendo o cruzamento entre os datasets usando os semi-identificadores em comum')
print('''
* Data de nascimento
* Raça/Cor
* Gênero
* Município
      ''')

# %%
print('\n')
print("-"*100)
print("Normalizando cada coluna para junção")
print("* Para racaCor : A coluna racaCor do dataset covid está em caixa baixa, \nenquanto a coluna DS_COR_RACA do dataset cand_2020 está em caixa alta. Vou colocar ambas em caixa alta.")

# %%
covid = covid.with_columns(
    pl.col("racaCor").str.to_uppercase().alias("racaCor")
)

# %%
cand_2020.select(['DS_COR_RACA']).filter(pl.col('DS_COR_RACA').is_in(covid.select(['racaCor']).unique()["racaCor"].to_list())).unique()

# %%
print('* Para data de nascimento : A coluna dataNascimento do dataset covid está no formato YYYY-MM-DD, \nenquanto a coluna DT_NASCIMENTO do dataset cand_2020 está no formato DD/MM/YYYY. Vou colocar ambas no formato DD-MM-YYYY.')

# %%
covid = covid.with_columns(
    pl.col("dataNascimento")
      .str.strptime(pl.Date, "%Y-%m-%d")   # interpreta a string como data
      .dt.strftime("%d-%m-%Y")             # formata no novo padrão
      .alias("dataNascimento")
)
covid.head()

# %%
print("* Para municipios de ambos : Serão colocados em caixa alta")
covid = covid.with_columns(
    pl.col("municipioCaso").str.to_uppercase().alias("municipioCaso")
)
cand_2020 = cand_2020.with_columns(
    pl.col("NM_UE").str.to_uppercase().alias("NM_UE")
)

# %%
cand_2020 = cand_2020.with_columns(
    pl.col("DT_NASCIMENTO").str.replace('/','-').str.replace('/','-').alias("DT_NASCIMENTO")
)

# %%
cand_2020.head()

# %%
###
print('\n')
print("Criando query no DuckDB para fazer o cruzamento entre os datasets")

query = '''
    SELECT
        *
    FROM
        cand_2020 AS c
    INNER JOIN covid AS v
    ON 
        c.DT_NASCIMENTO   = v.dataNascimento
        AND c.DS_COR_RACA = v.racaCor
        AND c.DS_GENERO   = v.sexoCaso
        AND c.NM_UE       = v.municipioCaso
'''
print(query)

# %%
print("Resultaddo do cruzamento entre os datasets :")

result = duckdb.query(query).to_df()
print(result.head())

# %%
print("Ao total de registros:", result.shape[0])

# %%
print('Salvando dados no arquivo resultado.csv')
result.to_csv('resultado.csv', index=False)


