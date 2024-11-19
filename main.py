from typing import Union
from fastapi import FastAPI

import json

import csv
import sqlite3

#import pandas as pd

#------------------------------- Cargos -------------------------------
dados_cargos = [
    {"cod_cargo": 1, "des_cargo": "Gerente de Setor", "salario_base": 11000, "nivel": "Gerente", "banco_horas": 1},
    {"cod_cargo": 2, "des_cargo": "Coordenador", "salario_base": 8500, "nivel": "Coordenador", "banco_horas": 1},
    {"cod_cargo": 3, "des_cargo": "Analista Superior", "salario_base": 6000, "nivel": "Analista", "banco_horas": 1},
    {"cod_cargo": 4, "des_cargo": "Analista Junior", "salario_base": 5000, "nivel": "Analista", "banco_horas": 1},
    {"cod_cargo": 5, "des_cargo": "Estagiário", "salario_base": 1100, "nivel": "Estagiário", "banco_horas": 0},
]

#------------------------------- Departamentos -------------------------------
dados_departamentos = [
    {"cod_departamento": 14, "nom_departamento": "Suporte e Manutenção", "cod_gerente": 2345, "andar": 6, "tarefa_alocada": "Auxílio em áreas relacionadas a hardware e software"},
    {"cod_departamento": 15, "nom_departamento": "Desenvolvimento de Software", "cod_gerente": 1122, "andar": 5, "tarefa_alocada": "Desenvolvimento e manutenção de aplicações"},
    {"cod_departamento": 16, "nom_departamento": "Infraestrutura de TI", "cod_gerente": 3202, "andar": 4, "tarefa_alocada": "Gerenciamento de servidores e redes"},
    {"cod_departamento": 17, "nom_departamento": "Segurança da Informação", "cod_gerente": 9432, "andar": 3, "tarefa_alocada": "Proteção de dados e prevenção de ameaças cibernéticas"},
    {"cod_departamento": 18, "nom_departamento": "Análise de Sistemas", "cod_gerente": 1456, "andar": 2, "tarefa_alocada": "Análise e projeto de sistemas de informação"},
    {"cod_departamento": 19, "nom_departamento": "Suporte Técnico", "cod_gerente": 5877, "andar": 1, "tarefa_alocada": "Atendimento a usuários e resolução de problemas técnicos"},
]

#------------------------------- Dependentes -------------------------------
dados_dependentes = [
    {"id_dependente": 1, "id_funcionario": 1220, "nome_dependente": "Mariana Matos", "grau_parentesco": "Filha", "idade": 10},
    {"id_dependente": 2, "id_funcionario": 1220, "nome_dependente": "Maria Matos", "grau_parentesco": "Filha", "idade": 8},
    {"id_dependente": 3, "id_funcionario": 1220, "nome_dependente": "Ana Matos", "grau_parentesco": "Esposa", "idade": 35},
    {"id_dependente": 4, "id_funcionario": 1122, "nome_dependente": "Pedro Prato", "grau_parentesco": "Filho", "idade": 12},
    {"id_dependente": 5, "id_funcionario": 1122, "nome_dependente": "Ana Prato", "grau_parentesco": "Filha", "idade": 9},
    {"id_dependente": 6, "id_funcionario": 1122, "nome_dependente": "Carlos Prato", "grau_parentesco": "Marido", "idade": 40},
    {"id_dependente": 7, "id_funcionario": 3202, "nome_dependente": "Marcos Moraes", "grau_parentesco": "Filho", "idade": 7},
    {"id_dependente": 8, "id_funcionario": 3202, "nome_dependente": "Fernando Moraes", "grau_parentesco": "Filho", "idade": 5},
    {"id_dependente": 9, "id_funcionario": 9432, "nome_dependente": "Ricardo Risso", "grau_parentesco": "Filho", "idade": 15},
    {"id_dependente": 10, "id_funcionario": 9432, "nome_dependente": "Juliana Risso", "grau_parentesco": "Filha", "idade": 13},
    {"id_dependente": 11, "id_funcionario": 9432, "nome_dependente": "Maria Risso", "grau_parentesco": "Mãe", "idade": 60},
    {"id_dependente": 12, "id_funcionario": 2345, "nome_dependente": "Carlos Bataille", "grau_parentesco": "Filho", "idade": 11},
    {"id_dependente": 13, "id_funcionario": 2345, "nome_dependente": "Laura Bataille", "grau_parentesco": "Filha", "idade": 8},
    {"id_dependente": 14, "id_funcionario": 2345, "nome_dependente": "Ana Bataille", "grau_parentesco": "Esposa", "idade": 37},
    {"id_dependente": 15, "id_funcionario": 7586, "nome_dependente": "Tiago Carvalho", "grau_parentesco": "Avó", "idade": 70},
    {"id_dependente": 16, "id_funcionario": 7586, "nome_dependente": "Serafina Carvalho", "grau_parentesco": "Avó", "idade": 68},
    {"id_dependente": 17, "id_funcionario": 5798, "nome_dependente": "Gustavo Dumas", "grau_parentesco": "Filha", "idade": 6},
    {"id_dependente": 18, "id_funcionario": 5798, "nome_dependente": "Sofia Dumas", "grau_parentesco": "Filha", "idade": 4},
    {"id_dependente": 19, "id_funcionario": 5798, "nome_dependente": "Paulo Dumas", "grau_parentesco": "Pai", "idade": 42},
    {"id_dependente": 20, "id_funcionario": 5798, "nome_dependente": "Clara Dumas", "grau_parentesco": "Mãe", "idade": 40},
    {"id_dependente": 21, "id_funcionario": 1456, "nome_dependente": "Pedro Queiroz", "grau_parentesco": "Filho", "idade": 5},
    {"id_dependente": 22, "id_funcionario": 1456, "nome_dependente": "Mauro Queiroz", "grau_parentesco": "Pai", "idade": 65},
    {"id_dependente": 23, "id_funcionario": 1456, "nome_dependente": "José Queiroz", "grau_parentesco": "Avó", "idade": 90},
    {"id_dependente": 24, "id_funcionario": 5877, "nome_dependente": "Leonardo Bertrand", "grau_parentesco": "Filho", "idade": 3},
    {"id_dependente": 25, "id_funcionario": 5877, "nome_dependente": "Beatriz Bertrand", "grau_parentesco": "Filha", "idade": 1},
    {"id_dependente": 26, "id_funcionario": 5645, "nome_dependente": "Thiago Adams", "grau_parentesco": "Filho", "idade": 7},
    {"id_dependente": 27, "id_funcionario": 5645, "nome_dependente": "Carla Adams", "grau_parentesco": "Filha", "idade": 5},
    {"id_dependente": 28, "id_funcionario": 5645, "nome_dependente": "Luís Adams", "grau_parentesco": "Pai", "idade": 45},
    {"id_dependente": 29, "id_funcionario": 1245, "nome_dependente": "Gabriel Maguire", "grau_parentesco": "Filho", "idade": 9},
    {"id_dependente": 30, "id_funcionario": 1245, "nome_dependente": "Samantha Maguire", "grau_parentesco": "Filha", "idade": 6},
    {"id_dependente": 31, "id_funcionario": 1245, "nome_dependente": "Ana Maguire", "grau_parentesco": "Mãe", "idade": 38},
    {"id_dependente": 32, "id_funcionario": 1245, "nome_dependente": "Ricardo Maguire", "grau_parentesco": "Pai", "idade": 40},
    {"id_dependente": 33, "id_funcionario": 1905, "nome_dependente": "Clara Conan", "grau_parentesco": "Filha", "idade": 8},
    {"id_dependente": 34, "id_funcionario": 1905, "nome_dependente": "Roberto Conan", "grau_parentesco": "Filho", "idade": 5},
    {"id_dependente": 35, "id_funcionario": 1905, "nome_dependente": "Elisabeth Conan", "grau_parentesco": "Avó", "idade": 72},
]

#------------------------------- Funcionários -------------------------------
dados_funcionarios = [
    {"id_funcionario": 1122, "nome_funcionario": "Carol Prato", "cod_cargo": 3, "cod_departamento": 15, "salario": 6400, "horas_trabalhadas_mes": 280},
    {"id_funcionario": 1220, "nome_funcionario": "Lucas Henrique Matos", "cod_cargo": 2, "cod_departamento": 14, "salario": 8700, "horas_trabalhadas_mes": 300},
    {"id_funcionario": 1245, "nome_funcionario": "Gregory Maguire", "cod_cargo": 4, "cod_departamento": 16, "salario": 5200, "horas_trabalhadas_mes": 255},
    {"id_funcionario": 1456, "nome_funcionario": "Eca de Queiroz", "cod_cargo": 5, "cod_departamento": 19, "salario": 1300, "horas_trabalhadas_mes": 120},
    {"id_funcionario": 1905, "nome_funcionario": "Arthur Conan", "cod_cargo": None, "cod_departamento": 16, "salario": 5200, "horas_trabalhadas_mes": 255},
    {"id_funcionario": 2345, "nome_funcionario": "Marion Bataille", "cod_cargo": 1, "cod_departamento": 14, "salario": 11500, "horas_trabalhadas_mes": 310},
    {"id_funcionario": 3202, "nome_funcionario": "Rafael Moraes", "cod_cargo": 3, "cod_departamento": 16, "salario": 6200, "horas_trabalhadas_mes": 260},
    {"id_funcionario": 5645, "nome_funcionario": "Douglas Adams", "cod_cargo": 5, "cod_departamento": 15, "salario": 1400, "horas_trabalhadas_mes": 120},
    {"id_funcionario": 5798, "nome_funcionario": "Alexandre Dumas", "cod_cargo": 4, "cod_departamento": 18, "salario": 5100, "horas_trabalhadas_mes": 222},
    {"id_funcionario": 5877, "nome_funcionario": "Leonel Bertrand", "cod_cargo": 4, "cod_departamento": 19, "salario": 5000, "horas_trabalhadas_mes": 240},
    {"id_funcionario": 7586, "nome_funcionario": "Mario de Carvalho", "cod_cargo": 4, "cod_departamento": 15, "salario": 5200, "horas_trabalhadas_mes": 200},
    {"id_funcionario": 9432, "nome_funcionario": "Carina Risso", "cod_cargo": 4, "cod_departamento": 17, "salario": 5100, "horas_trabalhadas_mes": 231},
    {"id_funcionario": 1908, "nome_funcionario": "Lionel Shriver", "cod_cargo": 4, "cod_departamento": 4, "salario": 5000, "horas_trabalhadas_mes": 198},
]

#------------------------------- Histórico de Salário -------------------------------
dados_historico_salarios = [
    {"id_historico": 1, "id_funcionario": 1220, "mes": "01/04/2024", "salario": 8500},
    {"id_historico": 2, "id_funcionario": 1220, "mes": "01/05/2024", "salario": 8500},
    {"id_historico": 3, "id_funcionario": 1220, "mes": "01/06/2024", "salario": 8500},
    {"id_historico": 4, "id_funcionario": 1220, "mes": "01/07/2024", "salario": 8500},
    {"id_historico": 5, "id_funcionario": 1220, "mes": "01/08/2024", "salario": 8500},
    {"id_historico": 6, "id_funcionario": 1220, "mes": "01/09/2024", "salario": 8700},
    {"id_historico": 7, "id_funcionario": 1122, "mes": "01/04/2024", "salario": 6000},
    {"id_historico": 8, "id_funcionario": 1122, "mes": "01/05/2024", "salario": 6000},
    {"id_historico": 9, "id_funcionario": 1122, "mes": "01/06/2024", "salario": 6000},
    {"id_historico": 10, "id_funcionario": 1122, "mes": "01/07/2024", "salario": 6000},
    {"id_historico": 11, "id_funcionario": 1122, "mes": "01/08/2024", "salario": 6000},
    {"id_historico": 12, "id_funcionario": 1122, "mes": "01/09/2024", "salario": 6400},
    {"id_historico": 13, "id_funcionario": 2345, "mes": "01/04/2024", "salario": 11000},
    {"id_historico": 14, "id_funcionario": 2345, "mes": "01/05/2024", "salario": 11000},
    {"id_historico": 15, "id_funcionario": 2345, "mes": "01/06/2024", "salario": 11000},
    {"id_historico": 16, "id_funcionario": 2345, "mes": "01/07/2024", "salario": 11200},
    {"id_historico": 17, "id_funcionario": 2345, "mes": "01/08/2024", "salario": 11500},
    {"id_historico": 18, "id_funcionario": 2345, "mes": "01/09/2024", "salario": 11500},
    {"id_historico": 19, "id_funcionario": 3202, "mes": "01/04/2024", "salario": 6000},
    {"id_historico": 20, "id_funcionario": 3202, "mes": "01/05/2024", "salario": 6000},
    {"id_historico": 21, "id_funcionario": 3202, "mes": "01/06/2024", "salario": 6100},
    {"id_historico": 22, "id_funcionario": 3202, "mes": "01/07/2024", "salario": 6100},
    {"id_historico": 23, "id_funcionario": 3202, "mes": "01/08/2024", "salario": 6200},
    {"id_historico": 24, "id_funcionario": 3202, "mes": "01/09/2024", "salario": 6200},
    {"id_historico": 25, "id_funcionario": 9432, "mes": "01/04/2024", "salario": 5000},
    {"id_historico": 26, "id_funcionario": 9432, "mes": "01/05/2024", "salario": 5000},
    {"id_historico": 27, "id_funcionario": 9432, "mes": "01/06/2024", "salario": 5000},
    {"id_historico": 28, "id_funcionario": 9432, "mes": "01/07/2024", "salario": 5000},
    {"id_historico": 29, "id_funcionario": 9432, "mes": "01/08/2024", "salario": 5000},
    {"id_historico": 30, "id_funcionario": 9432, "mes": "01/09/2024", "salario": 5000},
    {"id_historico": 31, "id_funcionario": 7586, "mes": "01/04/2024", "salario": 5000},
    {"id_historico": 32, "id_funcionario": 7586, "mes": "01/05/2024", "salario": 5000},
    {"id_historico": 33, "id_funcionario": 7586, "mes": "01/06/2024", "salario": 5000},
    {"id_historico": 34, "id_funcionario": 7586, "mes": "01/07/2024", "salario": 5100},
    {"id_historico": 35, "id_funcionario": 7586, "mes": "01/08/2024", "salario": 5100},
    {"id_historico": 36, "id_funcionario": 7586, "mes": "01/09/2024", "salario": 5200},
    {"id_historico": 37, "id_funcionario": 5798, "mes": "01/04/2024", "salario": 5000},
    {"id_historico": 38, "id_funcionario": 5798, "mes": "01/05/2024", "salario": 5000},
    {"id_historico": 39, "id_funcionario": 5798, "mes": "01/06/2024", "salario": 5000},
    {"id_historico": 40, "id_funcionario": 5798, "mes": "01/07/2024", "salario": 5100},
    {"id_historico": 41, "id_funcionario": 5798, "mes": "01/08/2024", "salario": 5100},
    {"id_historico": 42, "id_funcionario": 5798, "mes": "01/09/2024", "salario": 5200},
    {"id_historico": 43, "id_funcionario": 1456, "mes": "01/04/2024", "salario": 1300},
    {"id_historico": 44, "id_funcionario": 1456, "mes": "01/05/2024", "salario": 1300},
    {"id_historico": 45, "id_funcionario": 1456, "mes": "01/06/2024", "salario": 1300},
    {"id_historico": 46, "id_funcionario": 1456, "mes": "01/07/2024", "salario": 1300},
    {"id_historico": 47, "id_funcionario": 1456, "mes": "01/08/2024", "salario": 1300},
    {"id_historico": 48, "id_funcionario": 1456, "mes": "01/09/2024", "salario": 1300},
    {"id_historico": 49, "id_funcionario": 1905, "mes": "01/04/2024", "salario": 5000},
    {"id_historico": 50, "id_funcionario": 1905, "mes": "01/05/2024", "salario": 5000},
    {"id_historico": 51, "id_funcionario": 1905, "mes": "01/06/2024", "salario": 5000},
    {"id_historico": 52, "id_funcionario": 1905, "mes": "01/07/2024", "salario": 5000},
    {"id_historico": 53, "id_funcionario": 1905, "mes": "01/08/2024", "salario": 5000},
    {"id_historico": 54, "id_funcionario": 1905, "mes": "01/09/2024", "salario": 5000},
]

#------------------------------- Projetos Desenvolvidos -------------------------------
dados_projetos = [
    {
        "id_projeto": 1,
        "nome_projeto": "Desenvolvimento de Sistema Interno",
        "descricao": "Desenvolvimento de um sistema para gestão de recursos internos.",
        "data_inicio": "2024-01-10",
        "data_conclusao": "2024-06-15",
        "id_funcionario_responsavel": 1122,
        "custo": 25000,
        "status": "Concluido"
    },
    {
        "id_projeto": 2,
        "nome_projeto": "Migração de Servidores",
        "descricao": "Migração dos servidores para uma nova infraestrutura.",
        "data_inicio": "2024-02-20",
        "data_conclusao": None,
        "id_funcionario_responsavel": 3202,
        "custo": 15000,
        "status": "Em Execucao"
    },
    {
        "id_projeto": 3,
        "nome_projeto": "Plano de Segurança de Dados",
        "descricao": "Elaboração de um plano estratégico de segurança.",
        "data_inicio": "2024-03-05",
        "data_conclusao": "2024-10-10",
        "id_funcionario_responsavel": 9432,
        "custo": 20000,
        "status": "Concluido"
    },
    {
        "id_projeto": 4,
        "nome_projeto": "Desenvolvimento de API",
        "descricao": "Desenvolvimento de uma API para integração com sistemas externos.",
        "data_inicio": "2024-07-01",
        "data_conclusao": None,
        "id_funcionario_responsavel": 1220,
        "custo": 12000,
        "status": "Em Planejamento"
    },
    {
        "id_projeto": 5,
        "nome_projeto": "Análise de Dados de Vendas",
        "descricao": "Projeto para análise de dados de vendas para otimização de estoque.",
        "data_inicio": "2024-05-01",
        "data_conclusao": "2024-08-15",
        "id_funcionario_responsavel": 2345,
        "custo": 18000,
        "status": "Concluido"
    }
]

#------------------------------- Recursos do Projeto -------------------------------
dados_recursos = [
    {"id_recurso": 1, "id_projeto": 1, "descricao": "Licenças de software de desenvolvimento", "tipo_recurso": "financeiro", "quantidade_utilizada": 10000, "data_utilizacao": "2024-04-01"},
    {"id_recurso": 2, "id_projeto": 1, "descricao": "Desenvolvedores terceirizados", "tipo_recurso": "humano", "quantidade_utilizada": 5, "data_utilizacao": "2024-04-05"},
    {"id_recurso": 3, "id_projeto": 2, "descricao": "Servidores dedicados para teste", "tipo_recurso": "material", "quantidade_utilizada": 3, "data_utilizacao": "2024-05-01"},
    {"id_recurso": 4, "id_projeto": 2, "descricao": "Consultoria de segurança cibernética", "tipo_recurso": "humano", "quantidade_utilizada": 1, "data_utilizacao": "2024-05-10"},
    {"id_recurso": 5, "id_projeto": 3, "descricao": "Equipamentos de rede e cabos", "tipo_recurso": "material", "quantidade_utilizada": 50, "data_utilizacao": "2024-06-15"},
    {"id_recurso": 6, "id_projeto": 3, "descricao": "Contratação de especialistas em rede", "tipo_recurso": "humano", "quantidade_utilizada": 3, "data_utilizacao": "2024-06-18"},
    {"id_recurso": 7, "id_projeto": 3, "descricao": "Materiais de treinamento para equipe", "tipo_recurso": "material", "quantidade_utilizada": 30, "data_utilizacao": "2024-06-20"},
    {"id_recurso": 8, "id_projeto": 4, "descricao": "Contratação de empresa de backup de dados", "tipo_recurso": "financeiro", "quantidade_utilizada": 20000, "data_utilizacao": "2024-07-01"},
    {"id_recurso": 9, "id_projeto": 4, "descricao": "Analistas de segurança temporários", "tipo_recurso": "humano", "quantidade_utilizada": 2, "data_utilizacao": "2024-07-05"},
    {"id_recurso": 10, "id_projeto": 5, "descricao": "Serviços de design de interface", "tipo_recurso": "financeiro", "quantidade_utilizada": 15000, "data_utilizacao": "2024-08-01"},
    {"id_recurso": 11, "id_projeto": 5, "descricao": "Consultoria em UX/UI", "tipo_recurso": "humano", "quantidade_utilizada": 1, "data_utilizacao": "2024-08-10"},
    {"id_recurso": 12, "id_projeto": 5, "descricao": "Material de teste para interface de usuário", "tipo_recurso": "material", "quantidade_utilizada": 15, "data_utilizacao": "2024-08-15"},
    {"id_recurso": 13, "id_projeto": 3, "descricao": "Equipamentos para suporte técnico", "tipo_recurso": "material", "quantidade_utilizada": 10, "data_utilizacao": "2024-09-01"},
    {"id_recurso": 14, "id_projeto": 2, "descricao": "Assistentes técnicos temporários", "tipo_recurso": "humano", "quantidade_utilizada": 2, "data_utilizacao": "2024-09-10"},
    {"id_recurso": 15, "id_projeto": 2, "descricao": "Materiais de treinamento em hardware", "tipo_recurso": "material", "quantidade_utilizada": 20, "data_utilizacao": "2024-09-12"},
    {"id_recurso": 16, "id_projeto": 5, "descricao": "Consultoria para análise de dados", "tipo_recurso": "humano", "quantidade_utilizada": 2, "data_utilizacao": "2024-10-01"},
    {"id_recurso": 17, "id_projeto": 1, "descricao": "Software de análise de dados", "tipo_recurso": "financeiro", "quantidade_utilizada": 12000, "data_utilizacao": "2024-10-05"},
    {"id_recurso": 18, "id_projeto": 4, "descricao": "Compra de licenças de software antivírus", "tipo_recurso": "financeiro", "quantidade_utilizada": 8000, "data_utilizacao": "2024-11-01"},
    {"id_recurso": 19, "id_projeto": 2, "descricao": "Contratação de técnicos para instalação de segurança", "tipo_recurso": "humano", "quantidade_utilizada": 2, "data_utilizacao": "2024-11-05"},
    {"id_recurso": 20, "id_projeto": 4, "descricao": "Equipamentos de monitoramento de rede", "tipo_recurso": "material", "quantidade_utilizada": 25, "data_utilizacao": "2024-12-01"},
]

# Gerar o arquivo .csv
def salvar_csv(nome_arquivo, dados):
    with open(nome_arquivo, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=dados[0].keys())
        writer.writeheader()
        writer.writerows(dados)

# Salvando os dados em arquivos CSV
salvar_csv('Departamentos.csv', dados_departamentos)
salvar_csv('Dependentes.csv', dados_dependentes)
salvar_csv('Funcionarios.csv', dados_funcionarios)
salvar_csv('Historico_salarios.csv', dados_historico_salarios)
salvar_csv('Cargos.csv', dados_cargos)
salvar_csv('Projetos.csv', dados_projetos)
salvar_csv('Recursos.csv', dados_recursos)


# função para ler os arquivos csv criados
def ler_arquivo_csv(nome_arquivo):
    with open(nome_arquivo, mode="r", newline="") as csvfile:
        leitor_csv = csv.DictReader(csvfile)
        dados = [linha for linha in leitor_csv]
    return dados

# pegando os dados dos arquivos csv
dados_cargos = ler_arquivo_csv("Cargos.csv")
dados_departamentos = ler_arquivo_csv("Departamentos.csv")
dados_dependentes = ler_arquivo_csv("Dependentes.csv")
dados_funcionarios = ler_arquivo_csv("Funcionarios.csv")
dados_historico_salarios = ler_arquivo_csv("Historico_salarios.csv")
dados_projetos = ler_arquivo_csv("Projetos.csv")
dados_recursos = ler_arquivo_csv("Recursos.csv")

#----------------------------- Consultas com SQL -----------------------------
conn = sqlite3.connect('novo_banco.db')
cursor = conn.cursor()

'''
# estava dando erro, ai mandei apagar e refazer e parou
cursor.execute('DROP TABLE IF EXISTS Cargos')
cursor.execute('DROP TABLE IF EXISTS Departamentos')
cursor.execute('DROP TABLE IF EXISTS Funcionarios')
cursor.execute('DROP TABLE IF EXISTS Dependentes')
cursor.execute('DROP TABLE IF EXISTS Historico_salarios')
cursor.execute('DROP TABLE IF EXISTS Projetos')
cursor.execute('DROP TABLE IF EXISTS Recursos')
'''

# Criar tabelas
cursor.execute('''
CREATE TABLE IF NOT EXISTS Cargos (
    cod_cargo INTEGER PRIMARY KEY AUTOINCREMENT,
    des_cargo TEXT NOT NULL,
    salario_base FLOAT NOT NULL,
    nivel TEXT NOT NULL,
    banco_horas BOOLEAN
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Departamentos (
    cod_departamento INTEGER PRIMARY KEY AUTOINCREMENT,
    nom_departamento TEXT NOT NULL,
    cod_gerente INTEGER NOT NULL,
    andar INTEGER,
    tarefa_alocada TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dependentes (
    id_dependente INTEGER PRIMARY KEY,
    id_funcionario INTEGER,
    nome_dependente TEXT,
    grau_parentesco TEXT,
    idade INTEGER,
    FOREIGN KEY (id_funcionario) REFERENCES Funcionarios(id_funcionario)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Funcionarios (
    id_funcionario INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_funcionario TEXT NOT NULL,
    cod_cargo INTEGER,
    cod_departamento INTEGER,
    salario FLOAT NOT NULL,
    horas_trabalhadas_mes FLOAT NOT NULL,
    FOREIGN KEY(cod_cargo) REFERENCES Cargos(cod_cargo),
    FOREIGN KEY(cod_departamento) REFERENCES Departamentos(cod_departamento)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Historico_Salarios (
    id_historico INTEGER PRIMARY KEY,
    id_funcionario INTEGER,
    mes TEXT,
    salario FLOAT,
    FOREIGN KEY (id_funcionario) REFERENCES Funcionarios(id_funcionario)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Projetos (
    id_projeto INTEGER PRIMARY KEY,
    nome_projeto TEXT,
    descricao TEXT,
    data_inicio TEXT,
    data_conclusao TEXT,
    id_funcionario_responsavel INTEGER,
    custo FLOAT,
    status TEXT,
    FOREIGN KEY (id_funcionario_responsavel) REFERENCES Funcionarios(id_funcionario)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Recursos (
    id_recurso INTEGER PRIMARY KEY,
    id_projeto INTEGER,
    descricao TEXT,
    tipo_recurso TEXT,
    quantidade_utilizada INTEGER,
    data_utilizacao TEXT,
    FOREIGN KEY (id_projeto) REFERENCES Projetos(id_projeto)
)
''')

# Inserir dados na tabela Cargos
cursor.executemany('''
INSERT OR REPLACE INTO Cargos (cod_cargo, des_cargo, salario_base, nivel, banco_horas)
VALUES (:cod_cargo, :des_cargo, :salario_base, :nivel, :banco_horas)
''', [{'cod_cargo': cargo['cod_cargo'], 
        'des_cargo': cargo['des_cargo'], 
        'salario_base': cargo['salario_base'], 
        'nivel': cargo['nivel'], 
        'banco_horas': cargo['banco_horas']} 
       for cargo in dados_cargos])

# Inserir dados na tabela Departamentos
cursor.executemany('''
INSERT OR REPLACE INTO Departamentos (cod_departamento, nom_departamento, cod_gerente, andar, tarefa_alocada)
VALUES (:cod_departamento, :nom_departamento, :cod_gerente, :andar, :tarefa_alocada)
''', [{'cod_departamento': departamento['cod_departamento'], 
        'nom_departamento': departamento['nom_departamento'], 
        'cod_gerente': departamento['cod_gerente'], 
        'andar': departamento['andar'], 
        'tarefa_alocada': departamento['tarefa_alocada']} 
       for departamento in dados_departamentos])

# Inserir dados na tabela Dependentes
cursor.executemany('''
INSERT OR REPLACE INTO Dependentes (id_dependente, id_funcionario, nome_dependente, grau_parentesco, idade)
VALUES (:id_dependente, :id_funcionario, :nome_dependente, :grau_parentesco, :idade)
''', [{'id_dependente': dependente['id_dependente'], 
        'id_funcionario': dependente['id_funcionario'], 
        'nome_dependente': dependente['nome_dependente'], 
        'grau_parentesco': dependente['grau_parentesco'], 
        'idade': dependente['idade']} 
       for dependente in dados_dependentes])

# Inserir dados na tabela Funcionarios
cursor.executemany('''
INSERT OR REPLACE INTO Funcionarios (id_funcionario, nome_funcionario, cod_cargo, cod_departamento, salario, horas_trabalhadas_mes)
VALUES (:id_funcionario, :nome_funcionario, :cod_cargo, :cod_departamento, :salario, :horas_trabalhadas_mes)
''', [{'id_funcionario': funcionario['id_funcionario'], 
        'nome_funcionario': funcionario['nome_funcionario'], 
        'cod_cargo': funcionario['cod_cargo'], 
        'cod_departamento': funcionario['cod_departamento'], 
        'salario': funcionario['salario'], 
        'horas_trabalhadas_mes': funcionario['horas_trabalhadas_mes']} 
       for funcionario in dados_funcionarios])

# Inserir dados na tabela Historico_Salarios
cursor.executemany('''
INSERT OR REPLACE INTO Historico_Salarios (id_historico, id_funcionario, mes, salario)
VALUES (:id_historico, :id_funcionario, :mes, :salario)
''', [{'id_historico': historico['id_historico'], 
        'id_funcionario': historico['id_funcionario'], 
        'mes': historico['mes'], 
        'salario': historico['salario']} 
       for historico in dados_historico_salarios])

# Inserir dados na tabela Projetos
cursor.executemany('''
INSERT OR REPLACE INTO Projetos (id_projeto, nome_projeto, descricao, data_inicio, data_conclusao, id_funcionario_responsavel, custo, status)
VALUES (:id_projeto, :nome_projeto, :descricao, :data_inicio, :data_conclusao, :id_funcionario_responsavel, :custo, :status)
''', [{'id_projeto': projeto['id_projeto'], 
        'nome_projeto': projeto['nome_projeto'], 
        'descricao': projeto['descricao'], 
        'data_inicio': projeto['data_inicio'],
        'data_conclusao': projeto['data_conclusao'], 
        'id_funcionario_responsavel': projeto['id_funcionario_responsavel'], 
        'custo': projeto['custo'], 
        'status': projeto['status'],} 
       for projeto in dados_projetos])

# Inserir dados na tabela Recursos
cursor.executemany('''
INSERT OR REPLACE INTO Recursos (id_recurso, id_projeto, descricao, tipo_recurso, quantidade_utilizada, data_utilizacao)
VALUES (:id_recurso, :id_projeto, :descricao, :tipo_recurso, :quantidade_utilizada, :data_utilizacao)
''', [{'id_recurso': recurso['id_recurso'],
        'id_projeto': recurso['id_projeto'], 
        'descricao': recurso['descricao'],
        'tipo_recurso': recurso['tipo_recurso'],
        'quantidade_utilizada': recurso['quantidade_utilizada'], 
        'data_utilizacao': recurso['data_utilizacao'],} 
       for recurso in dados_recursos])

conn.commit()

# 1. Trazer a média dos salários (atual) dos funcionários responsáveis por projetos concluídos, agrupados por departamento.
 
query = '''
SELECT 
    AVG(f.salario) as media_salarios,
    d.nom_departamento
FROM 
    Funcionarios f
JOIN 
    Projetos p ON f.id_funcionario = p.id_funcionario_responsavel
JOIN 
    Departamentos d ON f.cod_departamento = d.cod_departamento
WHERE p.status = "Concluido"
GROUP BY nom_departamento
'''
cursor.execute(query)
resultados_ex1 = cursor.fetchall()

consulta1 =  [
        {"media_salarios": media_salarios, "departamento": departamento}
        for media_salarios, departamento in resultados_ex1
    ]

print("\n-------------------------------------Exercício 1---------------------------------\n")
print("Média dos salários dos funcionários responsáveis por projetos concluídos.")
for resultado in resultados_ex1:
    media_salarios, nom_departamento = resultado
    print(f'Média dos salários: {media_salarios}, Departamento: {nom_departamento}')
    

# 2. Identificar os três recursos materiais mais usados nos projetos, listando a descrição do recurso e a quantidade total usada.

query = '''
SELECT 
    r.id_recurso,
    r.descricao,
    SUM(r.quantidade_utilizada) AS total_utilizado
FROM 
    Recursos r
GROUP BY 
    r.id_recurso, r.descricao
ORDER BY 
    total_utilizado DESC
LIMIT 3;
'''
cursor.execute(query)
resultados_ex2 = cursor.fetchall()

consulta2 =  [
        {"id_recurso": id_recurso, "descricao": descricao, "total_utilizado": total_utilizado}
        for id_recurso, descricao, total_utilizado in resultados_ex2
    ]

print("\n-------------------------------------Exercício 2---------------------------------\n")
print("Três recursos materiais mais usados nos projetos")
for resultado in resultados_ex2:
    id_recurso, descricao, total_utilizado = resultado
    print(f'Id recurso: {id_recurso}, Descrição: {descricao}, Total utilizado: {total_utilizado}')

# 3. Calcular o custo total dos projetos por departamento, considerando apenas os projetos 'Concluídos'.

query = '''
SELECT 
    d.nom_departamento,
    SUM(p.custo) as custo_total
FROM 
    Projetos p
JOIN 
    Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
JOIN 
    Departamentos d ON f.cod_departamento = d.cod_departamento
WHERE 
    p.status = "Concluido"
GROUP BY 
    d.nom_departamento
'''
cursor.execute(query)
resultados_ex3 = cursor.fetchall()

consulta3 = [
    {"departamento": departamento, "custo_total": custo_total}
    for departamento, custo_total in resultados_ex3
]

print("\n-------------------------------------Exercício 3---------------------------------\n")
print("Custo total dos projetos por departamento, considerando apenas os projetos 'Concluídos'.")
for departamento, custo_total in resultados_ex3:
    print(f'Departamento: {departamento}, Custo Total: {custo_total:.2f}')


# 4. Listar todos os projetos com seus respectivos nomes, custo, data de início, data de conclusão e o nome do funcionário responsável, que estejam 'Em Execução'.

query = '''
SELECT 
    p.nome_projeto,
    p.descricao,
    p.data_inicio,
    p.data_conclusao,
    f.nome_funcionario,
    p.custo,
    p.status
FROM 
    Projetos p
JOIN 
    Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
WHERE 
    TRIM(p.status) = "Em Execucao";
'''
cursor.execute(query)
resultados_ex4 = cursor.fetchall()

consulta4 = [
        {
            "nome_projeto": nome_projeto,
            "descricao": descricao,
            "data_inicio": data_inicio,
            "data_conclusao": data_conclusao or "Não concluído",
            "nome_funcionario": nome_funcionario,
            "custo": custo,
            "status": status
        }
        for nome_projeto, descricao, data_inicio, data_conclusao, nome_funcionario, custo, status in resultados_ex4
    ]

print("\n-------------------------------------Exercício 4---------------------------------\n")
print("Todos os projetos 'Em execução'.")
for resultado in resultados_ex4:
    nome_projeto, descricao, data_inicio, data_conclusao, nome_funcionario, custo, status = resultado
    print(f'Nome do Projeto: {nome_projeto}, Descrição: {descricao}, '
          f'Data de início: {data_inicio}, Data de conclusão: {data_conclusao or "Não concluído"}, '
          f'Funcionário responsável: {nome_funcionario}, Custo: {custo}, '
          f'Status: {status}')

# 5. Identificar o projeto com o maior número de dependentes envolvidos, considerando que os dependentes são associados aos funcionários que estão gerenciando os projetos.

query = '''
SELECT 
    p.nome_projeto,
    COUNT(dep.id_dependente) AS total_dependentes
FROM 
    Projetos p
JOIN 
    Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
LEFT JOIN 
    Dependentes dep ON f.id_funcionario = dep.id_funcionario
GROUP BY 
    p.id_projeto
ORDER BY 
    total_dependentes DESC
LIMIT 1;
'''
cursor.execute(query)
resultados_ex5 = cursor.fetchall()

consulta5 = [
    {"nome_projeto": nome_projeto, "total_dependentes": total_dependentes}
    for nome_projeto, total_dependentes in resultados_ex5
]

print("\n-------------------------------------Exercício 5---------------------------------\n")
print("Projeto com o maior número de dependentes envolvidos.")
for resultado in resultados_ex5:
    nome_projeto, total_dependentes = resultado
    print(f'Projeto: {nome_projeto}, Total de dependentes: {total_dependentes}')


#----------------------------------FAST API----------------------------------
app = FastAPI()

#----------------------------------PRA ESTUDO----------------------------------
@app.get("/")
def read_root():
    return {"Hello": "World"}

# endpoint para funcionários
# http://127.0.0.1:8000/funcionarios/
@app.get("/funcionarios/")
async def get_funcionarios():
  return dados_funcionarios

# endpoint para departamentos
# http://127.0.0.1:8000/departamentos/
@app.get("/departamentos/")
async def get_departamentos():
  return dados_departamentos

# endpoint para dependentes
# http://127.0.0.1:8000/dependentes/
@app.get("/dependentes/")
async def get_dependentes():
  return dados_dependentes

# endpoint para historico de salários
# http://127.0.0.1:8000/historico_salarios/
@app.get("/historico_salarios/")
async def get_historico_salarios():
  return dados_historico_salarios

# endpoint para projetos
# http://127.0.0.1:8000/projetos/
@app.get("/projetos/")
async def get_projetos():
  return dados_projetos

# endpoint para recursos
# http://127.0.0.1:8000/recursos/
@app.get("/recursos/")
async def get_recursos():
  return dados_recursos

#----------------------------------CONSULTAS----------------------------------
# endpoint para conxulta 1
# http://127.0.0.1:8000/consulta1/
@app.get("/consulta1/")
async def get_ex1():
  return consulta1

# endpoint para conxulta 2
# http://127.0.0.1:8000/consulta2/
@app.get("/consulta2/")
async def get_ex2():
  return consulta2

# endpoint para conxulta 3
# http://127.0.0.1:8000/consulta3/
@app.get("/consulta3/")
async def get_ex3():
  return consulta3

#----------------------------------PRA ESTUDO----------------------------------
def salvar_json(nome_arquivo, dados):
    with open(nome_arquivo, 'w', encoding='utf-8') as jsonfile:
        json.dump(dados, jsonfile, ensure_ascii=False, indent=4)

# Exercício 1: Média de salários dos funcionários responsáveis por projetos concluídos
dados_ex1 = [{"media_salarios": media_salarios, "departamento": nom_departamento} 
             for media_salarios, nom_departamento in resultados_ex1]
salvar_json("media_salarios_departamentos.json", dados_ex1)

# Exercício 2: Três recursos mais usados nos projetos
dados_ex2 = [{"id_recurso": id_recurso, "descricao": descricao, "total_utilizado": total_utilizado}
             for id_recurso, descricao, total_utilizado in resultados_ex2]
salvar_json("recursos_mais_usados.json", dados_ex2)

# Exercício 3: Custo total dos projetos por departamento
dados_ex3 = [{"projeto": nome_projeto, "departamento": nome_departamento, "custo_total": custo_total}
             for nome_departamento, custo_total in resultados_ex3]
salvar_json("custo_total_projetos.json", dados_ex3)

# Exercício 4: Listar todos os projetos em execução
dados_ex4 = [{"projeto": nome_projeto, "descricao": descricao, "data_inicio": data_inicio, "data_conclusao": data_conclusao, "nome_funcionario": nome_funcionario, "custo": custo, "status": status}
             for nome_projeto, descricao, data_inicio, data_conclusao, nome_funcionario, custo, status in resultados_ex4]
salvar_json("projetos_em_execucao.json", dados_ex4)

# Exercício 5: Identificar o projeto com o maior número de dependentes envolvidos
dados_ex5 = [{"projeto": nome_projeto, "total_dependentes": total_dependentes}
             for nome_projeto, total_dependentes in resultados_ex5]
salvar_json("maior_num_dependentes.json", dados_ex5)

conn.commit()
conn.close()