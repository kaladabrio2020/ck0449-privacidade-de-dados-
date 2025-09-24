

# Trabalho 1: “Tem alguém aí?”


## Estrutura do Projeto

```
├── dataset/
│   ├── consulta_cand_2020_CE.csv   # Base de candidatos
│   ├── dados_covid-ce_trab02.csv   # Base de saúde
├── resultado.csv                   # Resultado do ataque (ligação entre candidatos e saúde)
├── trabalho1.py                    # Código-fonte em Python
├── README.md                       # Este arquivo
├── trabalho1.py                    # Onde foi feito o trabalho e depois passado para jupyter
├── trabalho1.html                  # Jupyter convertido em html

```

---

##  Tecnologias Utilizadas

* **Python 3.12+**
* **Polars** : manipulação de dados (mais rápido que Pandas).
* **DuckDB** : consultas SQL para cruzamento entre as bases.
* **Pandas** : Para salvar o resultado 
* **Numpy**  : 
* **pyarrow**
---

##  Passos da Implementação

1. **Carregamento dos datasets** (`consulta_cand_2020_CE.csv` e `dados_covid-ce_trab02.csv`).
2. **Seleção de colunas de interesse**:

   * *Candidatos*:

     * Identificadores: `NM_CANDIDATO`, `NR_CPF_CANDIDATO`
     * Semi-identificadores: `NM_UE`, `DT_NASCIMENTO`, `DS_GENERO`, `DS_COR_RACA`
   * *COVID*:

     * Semi-identificadores: `municipioCaso`, `bairroCaso`, `sexoCaso`, `dataNascimento`, `racaCor`
     * Sensíveis: `resultadoFinalExame` + 14 comorbidades
3. **Normalização dos dados**:

   * Datas unificadas para o formato `DD-MM-YYYY`.
   * Campos de cor/raça transformados para **caixa alta**.

4. **Cruzamento (INNER JOIN)** com DuckDB usando:

   * `DT_NASCIMENTO = dataNascimento`
   * `DS_COR_RACA = racaCor`
   * `DS_GENERO = sexoCaso`
   * `NM_UE = municipioCaso`

5. **Geração do resultado (`resultado.csv`)** contendo os candidatos e as possíveis informações de saúde inferidas.

---

## 📊 Resultado

O arquivo **resultado.csv** contém os registros de candidatos que puderam ser relacionados a registros da base de saúde.
É importante destacar que este resultado é uma **inferencia** e não uma certeza absoluta, uma vez que:

* Não sabemos se todos os candidatos realizaram testes no SUS.
* Algumas combinações de semi-identificadores podem não ser únicas.

---

## 📝 Como Executar

1. Instale as dependências:

   ```bash
   pip install polars duckdb pandas numpy pyarroy
   ```

2. Execute o script:

   ```bash
   python trabalho1.py
   ```
3. O resultado estará no arquivo:

   ```
   resultado.csv
   ```

> Caso não queira executar existe um .html com todo o progresso do codigo feito em jupyter