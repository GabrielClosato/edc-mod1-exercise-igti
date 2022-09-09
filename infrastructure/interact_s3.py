import boto3
import pandas as pd 

s3_client = boto3.client('s3')

s3_client.download_file("datalake-clo-igti-edc",
                        "data/alunos.csv",
                        "alunos.csv")

df = pd.read_csv("alunos.csv")
print(df)

s3_client.upload_file("ITENS_PROVA_2020.csv",
                      "datalake-clo-igti-edc",
                      "data/ITENS_PROVA_2020.csv")