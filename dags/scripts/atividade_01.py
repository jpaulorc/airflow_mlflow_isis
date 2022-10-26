"""
Atividade 01 - ETL utilizando o Pandas

Alunos:
    Bruno Alves dos Reis
    João Paulo Rodrigues Côrte
    Marcelo Moreira Ferreira da Silva

Professor:
    Jean Carlos Alves

Disciplina:
    CULTURA E PRÁTICAS DATAOPS E MLOPS
"""

# Importação de bibliotecas
import pandas as pd
from pandas_schema import Column, Schema
from pandas_schema.validation import InRangeValidation, InListValidation


def dataset_read():
    """Leitura do dados
    """
    download_url = 'https://drive.google.com/uc?export=download&id=1rZgVuwYon_3QogTr0-v48'\
                   '0PRpi-2l1-v'

    return pd.read_csv(download_url, header=0, sep=',', quotechar='"')


def schema_validate(data: pd.core.frame.DataFrame):
    """Validação dos dados"""
    schema = Schema([
        Column('sepal_length', [InRangeValidation(4.3, 7.9)]),
        Column('sepal_width', [InRangeValidation(2.0, 4.4)]),
        Column('petal_length', [InRangeValidation(1.0, 6.9)]),
        Column('petal_width', [InRangeValidation(0.1, 2.5)]),
        Column('classEncoder', [InRangeValidation(0, 2)]),
        Column('class', [InListValidation(['Iris-setosa', 'Iris-versicolor', 'Iris-virginica'])]),
    ])

    return schema.validate(data)


def save_correct_file(data, errors):
    """Salvando arquivo com valores corretos
    """
    df_ok = data.drop(data.index[[e.row for e in errors]])
    new_errors = schema_validate(df_ok)
    if ~len(new_errors):
        df_ok.to_parquet("data/pandas_arquivo_correto.parquet")


def save_incorrect_file(data, errors):
    """Salvando arquivo com valores incorretos
    """
    df_nok = data.loc[data.index[list(set([e.row for e in errors]))]]
    df_nok.sort_index(inplace=True)
    # adicionando coluna com mensagem de erro
    df_nok['messageError'] = pd.Series({e.row: e.column+' '+e.message for e in errors})
    df_nok.to_parquet("data/pandas_arquivo_incorreto.parquet")


def read_parquet_files():
    """Leitura e validação dos dados pós escrita dos arquivos
    """
    df_ok = pd.read_parquet('data/pandas_arquivo_correto.parquet')
    df_nok = pd.read_parquet('data/pandas_arquivo_incorreto.parquet')

    return df_ok, df_nok


df_iris = dataset_read()
errors = schema_validate(df_iris)
for error in errors:
    print(error)

save_correct_file(df_iris, errors)
save_incorrect_file(df_iris, errors)

df_ok, df_nok = read_parquet_files()

print("Dataset sem erros:")
print(df_ok.head())

print("Dataset com erros:")
print(df_nok.head())
