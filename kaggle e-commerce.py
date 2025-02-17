#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Após fazer o upload da base de dados no FileStore
file_location = "/FileStore/tables/ecommerce_dataset_updated.csv"
file_type = "csv"
from pyspark.sql.functions import sum, col, when, count, round, to_date, col, month, year
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import plotly.express as px
import plotly.graph_objects as go

infer_schema = "true"
delimiter = ","

df = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", "true")   .option("sep", delimiter)   .load(file_location)

# Renomeando algumas colunas
df = df.withColumnRenamed("Final_Price(Rs.)", "Final_Price")        .withColumnRenamed("Price (Rs.)", "Price")
       
# Alterando a coluna data de string pata date.
df = df.withColumn("Purchase_Date", to_date(col("Purchase_Date")))
display(df.limit(10))


# #### Venda Total e por mês

# In[ ]:


# Calcular a venda total
venda_total = df.select(round(sum("Final_Price"), 2).alias("Total_Vendas"))

# Criando as colunas month e year a partir da coluna Purchase_Date
mes_ano = df.withColumn("Month", month("Purchase_Date"))                      .withColumn("Year", year("Purchase_Date"))

# Agrupar por ano e mês e somar as vendas
sales_mes = mes_ano.groupBy("Year", "Month").agg(round(sum("Final_Price"), 2).alias("Total_Vendas_Mes"))

# Ordenar pelo mês (ano, mês)
sales_mes = sales_mes.orderBy(["Year", "Month"], ascending=[True, True])

# venda total e por mês
venda_total.show()
display(sales_mes)


# ##### Gráfico de vendas por mês

# In[ ]:


# Utilizando o Plotly para fazermos nosso primeiro gráfico com a biblioteca
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=sales_mes.toPandas()['Month'],
    y=sales_mes.toPandas()['Total_Vendas_Mes'],
    mode='lines+markers',  
    marker=dict(size=8, color='red', symbol='circle'), 
    line=dict(color='cyan', width=2),  
))

# Personalizar o layout
fig.update_layout(
    title='Total de Vendas por Mês',
    xaxis_title='Mês',
    yaxis_title='Total de Vendas',
    xaxis=dict(tickmode='array', tickvals=sales_mes.toPandas()['Month']),
    plot_bgcolor='black',  # Cor do fundo do gráfico
    paper_bgcolor='black',  # Cor do fundo da área do gráfico
    font=dict(color='white'),  # Cor da fonte para branco
    showlegend=False  # Remover legenda
)

fig.show()


# #### Vendas por categoria

# In[ ]:


df_vendas_categoria = df.groupBy("Category").agg(round(sum("Final_Price"), 2).alias("Vendas por Categoria"))
df_vendas_categoria = df_vendas_categoria.orderBy(F.col("Vendas por Categoria"), ascending=False)
display(df_vendas_categoria)


# ##### Gráfico de vendas por categoria

# In[ ]:


df_pandas = df_vendas_categoria.toPandas()
fig = px.bar(df_pandas, x="Category", y="Vendas por Categoria", title="Vendas por Categoria", color="Category")
fig.show()


# #### Desconto por categoria

# In[ ]:


# Converter a coluna de desconto para double para calcular em seguida o desconto
df = df.withColumn("Discount", col("Discount").cast("double"))

# Substituir valores NaN (null) por 0 caso tenha
df = df.fillna({"Discount": 0})

# Calcular o valor do desconto para cada item
df = df.withColumn("Discount_Amount", col("Price") - col("Final_Price"))

# Agrupar por categoria e calcular o total de desconto
desconto_por_categoria = df.groupBy("Category").agg(round(sum("Discount_Amount"), 2).alias("Desconto por Categoria"))

# Ordenar do maior para o menor desconto
desconto_por_categoria = desconto_por_categoria.orderBy(col("Desconto por Categoria").desc())
display(desconto_por_categoria)


# ##### Gráfico desconto por categoria

# In[ ]:


# Convertendo para o Pandas
df_descontos = desconto_por_categoria.toPandas()

fig = px.bar(df_descontos, x="Category", y="Desconto por Categoria", title="Total de Descontos por Categoria", text_auto=True)
fig.show()


# #### Total método de pagamento

# In[ ]:


# Contando a frequência de cada método de pagamento
df_metodos = df.groupBy("Payment_Method").agg(count("*").alias("Total Método de Pagamento"))

# Ordenar do maior para o menor
df_metodos = df_metodos.orderBy(col("Total Método de Pagamento").desc())

display(df_metodos)


# ##### Gráfico total método de pagamento

# In[ ]:


df_pagamento = df_metodos.toPandas()

# Criar gráfico de pizza
fig = px.pie(df_pagamento, names="Payment_Method", values="Total Método de Pagamento", title="Distribuição de Formas de Pagamento")
fig.show()

