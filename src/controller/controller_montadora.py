import pandas as pd
from model.montadoras import Montadora
from conexion.mongo_queries import MongoQueries

class Controller_Montadora:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_montadora(self) -> Montadora:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuario o novo CNPJ
        cnpj = input("CNPJ (Novo): ")

        if self.verifica_existencia_montadora(cnpj):
            # Solicita ao usuario a nova razão social
            razao_social = input("Razão Social (Novo): ")
            # Solicita ao usuario o novo nome fantasia
            nome_fantasia = input("Nome Fantasia (Novo): ")
            # Insere e persiste o novo montadora
            self.mongo.db["montadoras"].insert_one({"cnpj": cnpj, "razao_social": razao_social, "nome_fantasia": nome_fantasia})
            # Recupera os dados do novo montadora criado transformando em um DataFrame
            df_montadora = self.recupera_montadora(cnpj)
            # Cria um novo objeto montadora
            novo_montadora = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
            # Exibe os atributos do novo montadora
            print(novo_montadora.to_string())
            self.mongo.close()
            # Retorna o objeto novo_montadora para utilização posterior, caso necessário
            return novo_montadora
        else:
            self.mongo.close()
            print(f"O CNPJ {cnpj} já está cadastrado.")
            return None

    def atualizar_montadora(self) -> Montadora:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do montadora a ser alterado
        cnpj = int(input("CNPJ da montadora que deseja atualizar: "))

        # Verifica se o montadora existe na base de dados
        if not self.verifica_existencia_montadora(cnpj):
            # Solicita ao usuario a nova razão social
            razao_social = input("Razão Social (Novo): ")
            # Solicita ao usuario o novo nome fantasia
            nome_fantasia = input("Nome Fantasia (Novo): ")            
            # Atualiza o nome do montadora existente
            self.mongo.db["montadoras"].update_one({"cnpj":f"{cnpj}"},{"$set": {"razao_social":razao_social, "nome_fantasia":nome_fantasia}})
            # Recupera os dados do novo montadora criado transformando em um DataFrame
            df_montadora = self.recupera_montadora(cnpj)
            # Cria um novo objeto montadora
            montadora_atualizado = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
            # Exibe os atributos do novo montadora
            print(montadora_atualizado.to_string())
            self.mongo.close()
            # Retorna o objeto montadora_atualizado para utilização posterior, caso necessário
            return montadora_atualizado
        else:
            self.mongo.close()
            print(f"O CNPJ {cnpj} não existe.")
            return None

    def excluir_montadora(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o CPF do montadora a ser alterado
        cnpj = int(input("CNPJ da montadora que irá excluir: "))        

        # Verifica se o montadora existe na base de dados
        if not self.verifica_existencia_montadora(cnpj):            
            # Recupera os dados do novo montadora criado transformando em um DataFrame
            df_montadora = self.recupera_montadora(cnpj)
            # Revome o montadora da tabela
            self.mongo.db["montadoras"].delete_one({"cnpj":f"{cnpj}"})
            # Cria um novo objeto montadora para informar que foi removido
            montadora_excluido = Montadora(df_montadora.cnpj.values[0], df_montadora.razao_social.values[0], df_montadora.nome_fantasia.values[0])
            self.mongo.close()
            # Exibe os atributos do montadora excluído
            print("Montadora Removida com Sucesso!")
            print(montadora_excluido.to_string())
        else:
            self.mongo.close()
            print(f"O CNPJ {cnpj} não existe.")

    def verifica_existencia_montadora(self, cnpj:str=None, external:bool=False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo montadora criado transformando em um DataFrame
        df_montadora = pd.DataFrame(self.mongo.db["montadoras"].find({"cnpj":f"{cnpj}"}, {"cnpj": 1, "razao_social": 1, "nome_fantasia": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_montadora.empty

    def recupera_montadora(self, cnpj:str=None, external:bool=False) -> pd.DataFrame:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo cliente criado transformando em um DataFrame
        df_cliente = pd.DataFrame(list(self.mongo.db["montadoras"].find({"cnpj":f"{cnpj}"}, {"cnpj": 1, "razao_social": 1, "nome_fantasia": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_cliente