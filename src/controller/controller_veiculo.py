from bson import ObjectId
import pandas as pd
from model.veiculos import Veiculo
from conexion.mongo_queries import MongoQueries

class Controller_Veiculo:
    def __init__(self):
        self.mongo = MongoQueries()
        
    def inserir_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco
        self.mongo.connect()
        
        #Solicita ao usuario o novo modelo do veiculo
        modelo_novo_veiculo = input("Modelo (Novo): ")
        cor_novo_veiculo = input("Cor (Novo): ")
        tipo_combustivel_novo_veiculo = input("Tipo de Combustivel (Novo): ")
        proximo_veiculo = self.mongo.db["veiculos"].aggregate([
                                                    {
                                                        '$group': {
                                                            '_id': '$veiculos', 
                                                            'proximo_veiculo': {
                                                                '$max': '$codigo_veiculo'
                                                            }
                                                        }
                                                    }, {
                                                        '$project': {
                                                            'proximo_veiculo': {
                                                                '$sum': [
                                                                    '$proximo_veiculo', 1
                                                                ]
                                                            }, 
                                                            '_id': 0
                                                        }
                                                    }
                                                ])

        proximo_veiculo = int(list(proximo_veiculo)[0]['proximo_veiculo'])
        
        # Insere e Recupera o código do novo veiculo
        id_veiculo = self.mongo.db["veiculos"].insert_one({"codigo_veiculo": proximo_veiculo, "modelo_veiculo": modelo_novo_veiculo, "cor_veiculo": cor_novo_veiculo, "tipo_combustivel": tipo_combustivel_novo_veiculo})
        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = self.recupera_veiculo(id_veiculo.inserted_id)
        # Cria um novo objeto Veiculo
        novo_veiculo = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
        # Exibe os atributos do novo veiculo
        print(novo_veiculo.to_string())
        self.mongo.close()
        # Retorna o objeto novo_veiculo para utilização posterior, caso necessário
        return novo_veiculo

    def atualizar_veiculo(self) -> Veiculo:
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do veiculo a ser alterado
        codigo_veiculo = int(input("Código do Veiculo que irá alterar: "))        

        # Verifica se o veiculo existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):
            # Solicita a novo modelo do veiculo
            novo_modelo_veiculo = input("Modelo (Novo): ")
            nova_cor_veiculo = input("Cor (Novo): ")
            novo_tipo_combustivel_veiculo = input("Tipo Combustivel (Novo): ")
            # Atualiza a modelo do veiculo existente
            self.mongo.db["veiculos"].update_one({"codigo_veiculo": codigo_veiculo}, {"$set": {"modelo_veiculo": novo_modelo_veiculo, "cor_veiculo": nova_cor_veiculo, "tipo_combustivel": novo_tipo_combustivel_veiculo}})
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            # Cria um novo objeto Veiculo
            veiculo_atualizado = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
            # Exibe os atributos do novo veiculo
            print(veiculo_atualizado.to_string())
            self.mongo.close()
            # Retorna o objeto veiculo_atualizado para utilização posterior, caso necessário
            return veiculo_atualizado
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")
            return None

    def excluir_veiculo(self):
        # Cria uma nova conexão com o banco que permite alteração
        self.mongo.connect()

        # Solicita ao usuário o código do veiculo a ser alterado
        codigo_veiculo = int(input("Código do Veiculo que irá excluir: "))        

        # Verifica se o veiculo existe na base de dados
        if not self.verifica_existencia_veiculo(codigo_veiculo):            
            # Recupera os dados do novo veiculo criado transformando em um DataFrame
            df_veiculo = self.recupera_veiculo_codigo(codigo_veiculo)
            # Revome o veiculo da tabela
            self.mongo.db["veiculos"].delete_one({"codigo_veiculo": codigo_veiculo})
            # Cria um novo objeto Veiculo para informar que foi removido
            veiculo_excluido = Veiculo(df_veiculo.codigo_veiculo.values[0], df_veiculo.modelo_veiculo.values[0], df_veiculo.cor_veiculo.values[0], df_veiculo.tipo_combustivel.values[0])
            # Exibe os atributos do veiculo excluído
            print("Veiculo Removido com Sucesso!")
            print(veiculo_excluido.to_string())
            self.mongo.close()
        else:
            self.mongo.close()
            print(f"O código {codigo_veiculo} não existe.")

    def verifica_existencia_veiculo(self, codigo:int=None, external: bool = False) -> bool:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(self.mongo.db["veiculos"].find({"codigo_veiculo":codigo}, {"codigo_veiculo": 1, "modelo_veiculo": 1, "cor_veiculo": 1, "tipo_combustivel": 1, "_id": 0}))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo.empty

    def recupera_veiculo(self, _id:ObjectId=None) -> pd.DataFrame:
        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["veiculos"].find({"_id":_id}, {"codigo_veiculo": 1, "modelo_veiculo": 1, "cor_veiculo": 1, "tipo_combustivel": 1, "_id": 0})))
        return df_veiculo

    def recupera_veiculo_codigo(self, codigo:int=None, external: bool = False) -> pd.DataFrame:
        if external:
            # Cria uma nova conexão com o banco que permite alteração
            self.mongo.connect()

        # Recupera os dados do novo veiculo criado transformando em um DataFrame
        df_veiculo = pd.DataFrame(list(self.mongo.db["veiculos"].find({"codigo_veiculo":codigo}, {"codigo_veiculo": 1, "modelo_veiculo": 1, "cor_veiculo": 1, "tipo_combustivel": 1, "_id": 0})))

        if external:
            # Fecha a conexão com o Mongo
            self.mongo.close()

        return df_veiculo