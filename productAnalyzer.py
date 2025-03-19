from database import Database
from helper.writeAJson import writeAJson

db = Database(database="mercado", collection="compras")
#db.resetDatabase()

# 1- Total de vendas por dia:
result = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$cliente_id", "total": {"$sum": "$produtos.quantidade"}}},
    {"$group": {"_id": None, "soma": {"$sum": "$total"}}}
])
writeAJson(result, "Vendas por dia")

# 2- Produto mais vendido

result = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.descricao", "total": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"total": -1}},
    {"$limit": 1}
])

writeAJson(result, "Produto mais vendido")

# 3- Cliente que mais gastou em 1 compra

result = db.collection.aggregate([
     {"$unwind": "$produtos"},
     {"$group": {"_id": "$cliente_id", "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
     {"$sort": {"total": -1}},
    {"$limit": 1}

])
writeAJson(result, "Cliente que mais gastou em 1 compra")

# produtos vendidos em mais de 1 unidade
result = db.collection.aggregate([
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.descricao", "total": {"$sum": "$produtos.quantidade"}}},
    {"$match": {"total": {"$gt": 1}}},
    {"$sort": {"total": -1}}  
])

writeAJson(result, "Produtos vendidos acima de 1 unidade")

