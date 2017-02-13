#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Contruyendo la estructura de la red
#Este codigo explora la estructura y directorio de los datos de transparencia que se encuentran en el POT
#publicados por INAI en abril del 2016
#El código interactua con una base de datos en postgres, las consultas son basicas en SQL
#asi que es posible conectar otra base y sería necesario configurar el conector adecuado

#ESPECIFICACIONES
#Python 2 y 3
#Base de datos en Postgres
    #database="dir"
    #user="postgres"
    #password="postgres"
#Crear una carpeta llamada "data" para almacenar la salida de las dependencias


#LIBRERIAS Y PAQUETES
import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
import json
import csv
import psycopg2
import networkx as nx
import os
#import matplotlib.pyplot as plt
from networkx.readwrite import json_graph
from igraph import *
#A la base de datos se integraron datos de SEP e INE como atributos para cada nodo

#=========conecta a una base de datos, regresa el cursor========================
def connect_database(dbname,u,p):
    con=None
    try:
        con=psycopg2.connect(database=dbname, user=u, password=u)
        return con    
    except psycopg2.DatabaseError, e:
        print ('Error %s' % e    )
        sys.exit(1)
    return 0

#==================Contruye red por depenendecia con file=======================
#costruye la red de datos
#Consulta la tabla estructura y construye la red basado en la jerarquia que ocupa
#cada servidor publico
def explorando_red(con):
    f=open("output_redes.csv",'wb')
    cursor=con.cursor()
    #consulta para traer las diferentes dependencias
    query="select id, id_cargos,id_dependencia from dir_clean order by id_dependencia ASC"    
    cursor.execute(query)
    rows=cursor.fetchall()
    #escribe en el archibo para cada servidor y su cargo superior
    f.write("id,id_dependecia,id_cargos,superiores")
    for row in rows:   
        lista=[]
        lista=visited_node(row[1],row[2],lista,con)
        f.write("\n"+str(row[0])+","+row[2]+","+row[1]+",")
        lista=lista[:len(lista)-1]
        c=0        
        for i in lista:
            c=c+1
            if c<len(lista):
                f.write(str(i)+",")
            else:
                f.write(str(i))
            
#================visita cada nodo y busca el superior=======================                
#funcion recursiva que trae los id de los puestos superiores, con base en la tabla estrucura                
def visited_node(id,dependencia,lista,con):  
    cursor=con.cursor()    
    query="select id_cargo_superior from estructura where id_cargos like '"+str(id)+"' and id_dependencia like '"+str(dependencia)+"';"
    cursor.execute(query)
    rows=cursor.fetchall() 
    if len(rows)!=0:               
       if rows[0][0] not in lista:
            lista.append(rows[0][0])    
            visited_node(rows[0][0],dependencia,lista,con)        
    return lista
#===========================Red por dependencia==============================
#Crea una red por dependencia, en la base de datos hay cerca de 277
def net_by_dependecia(con):
    cursor=con.cursor()
    edges=[]
    #explorando el nodo superior aca cada nodo y trayendo el id
    #todas las dependencias
    query="Select distinct(id_dependencia) from estructura order by id_dependencia desc"    
    cursor.execute(query)
    dependencias=cursor.fetchall()    
    #id de de dependencias, lista
    for dep in dependencias:
        query="select id from dir_clean where id_dependencia= '"+str(dep[0])+"';"
        cursor.execute(query)
        nodos=cursor.fetchall()
        #Creamos el graph
        G=nx.Graph()
        a=[]
        #Creamos los nodos para agregar a la red
        for n in nodos:
            #Funcion que genera los atributos para cada nodo
            atributes=attributes(n[0],con) #genera el diccionario de atributos
            #Agragando el id y los atributos
            G.add_node(n[0],id_titulo=atributes['id_titulo'],id_institucion=atributes['id_institucion'],partido=atributes['partido'])#genera la lista de nodos y atributos                 
                      
        #obtenemos el cargo para cada nodo para poder buscar su superior
        query="select id,id_cargos from dir_clean where id_dependencia= '"+str(dep[0])+"';"
        cursor.execute(query)
        nodos=cursor.fetchall()    
        #EDGES
        #Ahora se generaran los edges
        for n in nodos:
            id_cargo_superior=who(n[1],dep[0],con)
            #buscando al funcionario de cargo superior
            query="select id from dir_clean where id_cargos='"+str(id_cargo_superior)+"' and id_dependencia ='"+str(dep[0])+"';"
            cursor.execute(query)
            ans=cursor.fetchall()
            if len(ans)>0:
                #para el nodo si riene un superior se calcula el peso
                wei=weight(attributes(n[0],con),attributes(ans[0][0],con))
                #Ya que fue calculado el peso se agrega al edge
                G.add_edge(n[0],ans[0][0], weight=wei)
                #si no tiene un superior se agrega un edge a su mismo con peso 0
            else:
                G.add_edge(n[0],n[0], weight=0)
                #edges.append([n[0],n[0]], weight=wi)        
    #para cada dependencia se construye una red  
    #DEVELOP GRAPH       
        #agregando los atributos               
        cursor.execute("select dependencia from estructura where id_dependencia like '"+str(dep[0])+"'")
        ans=cursor.fetchall()
        nombre_dependencia=ans[0][0]
        #llama la función de calcula las medidas
        measure=measurements(G,con,nombre_dependencia.decode(encoding='UTF-8',errors='strict'),str(dep[0]))
        
    #clasificando las carreras, reduciendo la lista con dedupe y calculando la distancia al más cercano. 
    
#===============Calculando peso para el edge =====================================
def weight(list1,list2):
    peso=0.5 #por pertenecer a la misma dependencia
    if list1['id_titulo']==list2['id_titulo']:
        peso=peso+0.15 #tienen la misma carrera(de hecho deberia ser mas fuerte si ademas estuvieron en la misma escuela)
    if (list1['id_institucion']==list2['id_institucion']):
        peso=peso+0.3    #estudiaron en la misma escuela
    if (list1['partido']==list2['partido']):
        peso=peso+0.05 # pertenecen al mismo partido, le reste importancia ya que no todos tienen afiliación política
    
    return peso
#=================Calcula los clique de la red===================================
# Calculando medidas de clique,community          
def community(G,con,nombre,id):
    #Based on the algorithm published by Bron & Kerbosch (1973) [R198] as adapated by Tomita,
    #Tanaka and Takahashi (2006) [R199] and discussed in Cazals and Karande (2008) [R200]. 
    #The method essentially unrolls the recursion used in the references to avoid issues of recursion stack depth.
    #This algorithm is not suitable for directed graphs.
    #This algorithm ignores self-loops and parallel edges as clique is not conventionally defined with such edges.
    #calcula todos los cliques
    
    cliques=list(nx.find_cliques(G))    
    #  Create the maximal clique graph of a graph.
    #Finds the maximal cliques and treats these as nodes. 
    #The nodes are connected if they have common members in the original graph. 
    #Theory has done a lot with clique graphs, but I haven’t seen much on maximal clique graphs.            
    maximal=list(nx.make_max_clique_graph(G))
    #This module provides functions and operations for bipartite graphs. 
    #Bipartite graphs B = (U, V, E) have two node sets U,V and edges in E 
    #that only connect nodes from opposite sets. It is common in the literature
    #to use an spatial analogy referring to the two node sets as top and bottom nodes.
    bipartite= list(nx.make_clique_bipartite(G, fpos=None, create_using=None, name=None))
    #obtiene el número de cliques en la red
    graph_clique_number=nx.graph_clique_number(G, cliques=None)
    #Componentes conectados
    component_connected_g=[]
    compo=nx.connected_component_subgraphs(G)
    for c in compo:
        component_connected_g.append(list(c))
        
    return cliques,maximal,bipartite,graph_clique_number,component_connected_g
   
#==============Calculo de medidas para la red===================================    
def measurements(G,con,nombre,id):
        #Configuración para el plot
        
        #plt.figure(figsize=(10,10))
        #plt.axis('off')  
        #plt.title(nombre)    
        nx.draw_spring(G, with_labels=True)
        nx.draw(G, with_labels=True)
        #calcula centrality
        central,top_de,top_clos,bet_top,top_de_c=centrality(G,con)
        #calculo del linkedin prediction
        linkedin,jackard,pref=linkedin_prediction(G)
        #calculo de clustering y cliques
        cliques,maximal,bipartite,graph_clique_number,component_connected_g=community(G,con,nombre,id)#AGREGUE ESTO    
        
        #Generando archivos JSON para salida de datos
        path=os.getcwd()+"/data/"+str(id)+".json" 
        with open(path, 'w') as outfile1:
            outfile1.write(json.dumps(json_graph.node_link_data(G)))
            
        path=os.getcwd()+"/data/"+str(id)+"_centrality.json" 
        with open(path, 'w') as outfile1:
            outfile1.write(json.dumps(central))   
                    
        path=os.getcwd()+"/data/"+str(id)+"_linkedin_prediction.json" 
        with open(path, 'w') as outfile1:
            outfile1.write(json.dumps(linkedin))
        
        path=os.getcwd()+"/data/"+str(id)+"_all.csv" 

        report(path,top_de,top_clos,bet_top,top_de_c,jackard,pref, cliques,maximal,bipartite,graph_clique_number,component_connected_g)


        path=os.getcwd()+"/data/"+str(id)+".gexf"                
        
        #clustering(G,con,nombre,id)
        #guardando la red con el id de la dependencia
        nx.write_gexf(G, path,encoding='utf-8')       
        path=os.getcwd()+"/images/"+str(id)
        #ploteando
        #plt.savefig(path)
        #plt.show()
def report(path,top_de,top_clos,bet_top,top_de_c,jackard,pref,cliques,maximal,bipartite,graph_clique_number,component_connected_g):             
        with open(path, "wb") as f:
            writer = csv.writer(f)
            writer.writerow(["*Análisis de Redes*"])
            writer.writerow(["================Centrality(Elite)==========="])
            writer.writerow(["*Top degree centrality*"])
            writer.writerows(top_de)
            
            writer.writerow(["*Top closeness*"])
            writer.writerows(top_clos)
            
            writer.writerow(["*Top Betweetness*"])
            writer.writerows(bet_top)
            
            writer.writerow(["*Top degree centrality*"])
            writer.writerows(top_de_c)
            
            
            writer.writerow(["==================Community================"])
            writer.writerow(["*Cliques*"])
            writer.writerows(cliques)
            writer.writerow(["*Maximal clique*"])

            writer.writerow(maximal)
            writer.writerow(["*Bipartite clique*"])
            writer.writerow(bipartite)
            writer.writerow(["*Graph clique number*"])
            
            writer.writerow([graph_clique_number])
            writer.writerow(["*Component connected Graph*"])
            writer.writerows(component_connected_g)
            
            writer.writerow(["==================Prediction================"])
            writer.writerow(["*Linkedin Prediction*"])
            writer.writerow(["*Jackard*"])
            writer.writerows(jackard)
            writer.writerow(["*Preferencial*"])
            writer.writerows(pref)


#================Linkedin Prediction=======================================
#Cuales son los nodos más probables a tener conexión en el futuro
def linkedin_prediction(G):
    #Link Prediction,Jaccard Coefficient
    #Top 5 que tienen más probabilidad de conectarse
    jackard=[]
    preds_jc = nx.jaccard_coefficient(G)
    pred_jc_dict = {}
    for u, v, p in preds_jc:
        pred_jc_dict[(u,v)] = p
    Jaccard_Coefficient=[]
    Jaccard_Coefficient_10=sorted(pred_jc_dict.items(), key=lambda x:x[1], reverse=True)[:10]
    for c in Jaccard_Coefficient_10:
        j={"Nodes":c[0],"probability":c[1]}
        Jaccard_Coefficient.append(j)
        jackard.append([c[0],c[1]])
    
    #Preferential attacment, top 5 más importante o reelevantes a conectarse
    preds_pa = nx.preferential_attachment(G)
    pref=[]
    pred_pa_dict = {}
    for u, v, p in preds_pa:
        pred_pa_dict[(u,v)] = p
    preferential=[]
    preferential_10=sorted(pred_pa_dict.items(), key=lambda x:x[1], reverse=True)[:10]
    for c in preferential_10:
        j={"Nodes":c[0],"measure":c[1]}
        preferential.append(j)
        pref.append([c[0],c[1]])
    dir={"Jaccard_Coefficient":Jaccard_Coefficient,"preferential":preferential}
    return dir,jackard,pref

    
#==============================Centrality=======================================
#Cuales son los nodos centrales en la red
def centrality(G,con):
    cursor=con.cursor()
    #Top 5 de nodos centrales
    centre=sorted(G.degree().items(), key=lambda x:x[1], reverse=True)[:5]    
    top_degree=[]
    top_de=[]
    #Busca quienes son los nodos centrales en la base de datos
    for c in centre:
        query="select nombre,primer_apellido,segundo_apellido from dir_clean where id ="+str(c[0])+""
        cursor.execute(query)
        ans=cursor.fetchall()
        top={"nombre":ans[0][0],"primer_apellido":ans[0][1],"segundo_apellido":ans[0][2],"top":c[1]}
        top_degree.append(top)
        top_de.append([ans[0][0],ans[0][1],ans[0][2],c[1]])
    #calcula el Closeness centrality para la red
    closeness_centrality = nx.closeness_centrality(G)
    closeness=[]
    #Top 5
    closs_5= sorted(closeness_centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    top_clos=[]
    for c in closs_5:
        clo={"id":c[0],"closeness":c[1]}
        closeness.append(clo)
        top_clos.append([c[0],c[1]])
    #Calcula el Betweeness centrality el top 5
    betweeness_centrality = nx.betweenness_centrality(G)
    betw_5=sorted(betweeness_centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    betweeness_centrality=[]
    bet_top=[]
    for c in betw_5:
        be={"id":c[0],"betweeness":c[1]}
        betweeness_centrality.append(be)
        bet_top.append([c[0],c[1]])
    #Closnes
    degree_centrality = nx.degree_centrality(G)
    #top centrality degree 
    top_de_c=[]
    top_degree_centrality=[]
    top_degree_centrality_5=sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    for c in top_degree_centrality_5:
        t={"id":c[0],"top_degree_centrality":c[1]}
        top_degree_centrality.append(t)    
        top_de_c.append([c[0],c[1]])
    #genera un diccionario para las medidas
    dir={"centrality":top_degree,"closeness":closeness,"betweeness_centrality":betweeness_centrality,"top_degree_centrality":top_degree_centrality}
    return dir,top_de,top_clos,bet_top,top_de_c

#===================Calcula el grafo central===============================
def centrality_graph_degree(G):
    #plt.figure(figsize=(10,10))
    #plt.axis('on')  
    deg=nx.degree(G)
    #h=plt.hist(deg.values(),100)
    #plt.loglog(h[1][1:],h[0])

#=====================Elimina nodos con un treashold de grado==================
#Si se desea eliminar nodos con poca conección
def trim_degrees(g, degree=2):
    g2=g.copy()
    d=nx.degree(g2)
    for n in g2.nodes():
        if d[n]<=degree:
            g2.remove_node(n)
    return g2        
#consulta los datos de cada nodo y genera un diccionario que será agregado como atributo a la red        

    
#==============================Atributes======================================
#Consulta la base de datos para traer los atributos para cada nodo e incluirlos
def attributes( id,con):    
    cursor=con.cursor()    
    query="select id_titulo,id_institucion from sep_ascii where id like '"+str(id)+"'";
    cursor.execute(query)
    ans=cursor.fetchall()
    #si existe la información
    if len(ans)>0:
        id_titulo=ans[0][0] #titulo profesional
        id_institucion=ans[0][1] #escuela de prosedencia
    else:
        id_titulo=""
        id_institucion=""
    query="select partido from dir_clean where id="+str(id)+"";
    cursor.execute(query)
    ans=cursor.fetchall()
    #consulta el partido
    if len(ans)>0:
        partido=ans[0][0]
    else:
        partido=""
    attribute={'id_titulo':str(id_titulo),'id_institucion':str(id_institucion),'partido':str(partido)}
    
    return attribute
    
    
#==================================Quien es el nodo superior===============================
#acorde al cargo se consulta en la tabla estructura quien es el cargo superior
def who(id_cargos,dependencia,con):
    cursor=con.cursor()
    query="select id_cargo_superior from estructura where id_cargos like '"+str(id_cargos)+"' and id_dependencia like '"+str(dependencia)+"';"
    cursor.execute(query)
    ans=cursor.fetchall()
    return ans[0][0]
#===============================Genera Red con un archivo============================
#Si se tiene una lista de ids de funcionarios se genera la red a partir de esta lista
def develop_net_dependencia_with_FIlE(file,con):
    cursor=con.cursor()
    g = Graph()
    edges=[]
    vertices=[]
    partidos=[]
    nombres=[]
    with open(file,'rb') as csvfile:
        reader=csv.reader(csvfile)
        for row in reader:
            query="select id,nombre,primer_apellido,segundo_apellido from dir_clean where id_cargos like '"+row[2]+"' and id_dependencia like '"+row[1]+"';"            
            vertices.append(str(row[0]))                        
            cursor.execute(query)
            result=cursor.fetchall()            
            for r in result:
                #print r[0],r[1]
                if row[0]!=r[0]:
                    edges.append((str(row[0]),str(r[0])))
                nombres.append(r[1])
                query="select id from partidos_ascii where nombre like upper('"+str(r[1])+"') and apellido_paterno like upper('"+r[2]+"') and apellido_materno like upper('"+r[3]+"');"
                #print query
                cursor.execute(query)
                results=cursor.fetchall()
                
                if len(results)>0:
                    query="select partido from partidos where id= "+str(results[0][0])
                    cursor.execute(query)
                    resu=cursor.fetchall() 
                    if len(resu)>0:
                        partidos.append(resu[0][0])
                else:
                    #print "no"
                    partidos.append("No")
            #si no esta en la base de datos
            if len(result)==0:
                print ("No encontre este",row[0])
            
     
    g.add_vertices(vertices)        
    g.add_edges(edges)
    g.vs
    #g.vs["nombres"]=vertices
    g.vs["partidos"]=partidos
    layout = g.layout("fr")
    visual_style = {}
    visual_style["layout"] = layout
    visual_style["vertex_size"] = 25
    visual_style["label_size"]=9
    visual_style["vertex_label"] =g.vs["label"]=vertices
    n=["PRI","PAN","PRD","No","MCI","NA","MOR","PVE"]
    c=["red","BLUE","YELLOW","GRAY","ORANGE","TURQUOISE","BROWN","green"]
    visual_style["legend"]=[1, 95, n,c]
    color_dict={"PRI":"red","PAN":"BLUE","PRD":"YELLOW","No":"GRAY","MCI":"ORANGE","NA":"TURQUOISE","NA.":"TURQUOISE","MOR":"BROWN","PVE":"green"}
   
    #visual_style["vertex_label"] = g.vertices
    visual_style["vertex_color"] = [color_dict[partido] for partido in g.vs["partidos"]]
    visual_style["bbox"] = (1000, 1000)
    plot(g, **visual_style)


#==================================INicio del programa========================
#Se pueden decomentar las funciones que se desean ejecutar
def start():
    database="dir"
    user="postgres"
    password="postgres"
    con=connect_database(database,user,password)
    #explorando_red(con)    
    #cleaning_data_estados(con) #limpia los datos de los estados y los integra
    #archivo="infoteq.csv"   #en caso de tener una lista de ids, agregar
    #develop_net_dependencia(archivo,con) #si se quiere construir la red por archivo
    net_by_dependecia(con)    #genera las redes para todas las dependencias y calcula las medidas
start()        



    