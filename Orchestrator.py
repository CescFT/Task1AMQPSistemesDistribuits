'''
Created on 20 de mar. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''
# -*- coding: utf-8 -*-
import sys, time, os, yaml, pika, re
from zipfile import ZipFile
from cos_backend import COS_Backend
from ibm_cf_connector import CloudFunctions

def callback(ch, method, properties, body):
    string=""
    for i in body:
        if i < 128:
            val=str(chr(i))
            string+=val
        else:
            lletra=tractar_lletra(i)
            string+=lletra
    if string == "stop":
        channel.stop_consuming()
    
def generar_funcions(nom_zip, invocador, nom_funcio):
    with open(nom_zip, 'rb') as funcio:
        bytes_f=funcio.read()
    v=invocador.create_action(nom_funcio, bytes_f, 'blackbox', 'ibmfunctions/action-python-v3.6', is_binary=True, overwrite=True)
    return v
def inicialitzar_objectStorage(storage_backend, elem=None):
    ll=storage_backend.list_objects('contenidortask1', elem)
    if len(ll) != 0:
        for i in ll:
                nomFitx=i['Key']
                storage_backend.delete_object('contenidortask1', nomFitx)


def ini_cloud_dades(object_storage):
    inicialitzar_objectStorage(object_storage, 'wordcount')
    inicialitzar_objectStorage(object_storage, 'countwords')
    inicialitzar_objectStorage(object_storage, 'resultat_finalAMQP.txt')

def ini_cloud_funcions(invo_funcions):
    
    tot_be=generar_funcions('wordCountAMQP.zip',invo_funcions, 'wordCountAMQP')
    if tot_be == 1:
        esborrar_pantalla()
        print('Sembla ser que no hi ha el fitxer wordCount.zip, no podem procedir a executar res. Abortar.')
        return 1
    tot_be=generar_funcions('reducercountwordsAMQP.zip',invo_funcions, 'reducer-countwordsAMQP')
    if tot_be == 1:
        esborrar_pantalla()
        print('Sembla ser que no hi ha el fitxer reducer.zip, no podem procedir a executar res. Abortar.')
        return 1
    return 0
def esborrar_pantalla():
    os.system('cls')

def ini_repositori_local(opcio):
    resfinaltxt='resultat_final.txt'
    respartotalstxt='paraules_totals.txt'
    zip_res_final='resultats_OrchestratorCues_Cristina_Francesc.zip'  
    if opcio == 'inici':
        if os.path.exists(resfinaltxt):
            os.remove(resfinaltxt)
        if os.path.exists(respartotalstxt):
            os.remove(respartotalstxt)
        if os.path.exists(zip_res_final):
            os.remove(zip_res_final)
    elif opcio == 'final':
        if os.path.exists(resfinaltxt):
            os.remove(resfinaltxt)
        if os.path.exists(respartotalstxt):
            os.remove(respartotalstxt)

def netejarObjectStorage(cloudOS):
    ll=cloudOS.list_objects('contenidortask1', 'wordcount')
    for diccinfo in ll:
        key=diccinfo['Key']
        cloudOS.delete_object('contenidortask1', key)
    print("Object Storage net. Hi ha resultats finals nomes.")
#PER DEBUGAR: ibmcloud fn activation poll (veure logs)
if __name__ == '__main__':
    if len(sys.argv) == 3:
        with open('ibm_cloud_config.txt', 'r') as config_file:
            conf = yaml.safe_load(config_file)
        objectStorage = COS_Backend(conf['ibm_cos'])
        functionInvocator=CloudFunctions(conf['ibm_cf'])
        amqp=conf['amqp']
        res=ini_cloud_funcions(functionInvocator)
        if res == 1:
            sys.exit()
        else:
            print("Generacio de funcions:OK.")
        ini_cloud_dades(objectStorage)
        print("Inicialitzacio del cos backend:OK.")
        ini_repositori_local('inici')
        print("Inicialitzacio del repositori local de la maquina:OK.")
        input("pressioni enter per a continuar")
        esborrar_pantalla()
        
        print("Enviant peticions de wordcounts...")
        num_maps = int(sys.argv[2])
        n = 0
        mida_bytes = objectStorage.head_object('contenidortask1', sys.argv[1])
        mida = mida_bytes['content-length']
        p = int(mida) / num_maps
        minim = int(p * n)
        regex = r"(\s)"
        ti = time.time()
        
        while n < num_maps:
            no_correcte = True
            despl = 1
            maxim = int(p * (n + 1))
            if n != (num_maps - 1):
                while no_correcte:
                    text = objectStorage.get_object('contenidortask1', sys.argv[1], extra_get_args = {'Range': 'bytes=' + str(maxim) + '-' + str(maxim)})
                    if not re.search(regex, str(text)):
                        maxim = maxim + 1
                        despl = despl + 1
                    else:
                        no_correcte = False
            else:
                maxim = int(mida)
            
            text = objectStorage.get_object('contenidortask1', sys.argv[1], extra_get_args = {'Range': 'bytes=' + str(minim) + '-' + str(maxim)})
            decoded_text = text.decode("latin1")
            params = {'credentials':conf['ibm_cos'], 'text':decoded_text, 'index':str(n+1), "url":conf['amqp']}
            functionInvocator.invoke('wordCountAMQP', params)
            n = n + 1
            minim = int((p * n) + despl)
        
        
        esborrar_pantalla()
        functionInvocator.invoke('reducer-countwordsAMQP', {"credentials":conf['ibm_cos'], "url":amqp, "num_maps":num_maps})
        print("Esperant a que el cloud acabi de treballar...")
        url = amqp.get("url")
        params = pika.URLParameters(url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel() # start a channel
        channel.queue_declare(queue='orchestrator')
        channel.basic_consume(callback, queue='orchestrator', no_ack=True)
        channel.start_consuming()
        tf=time.time()
        esborrar_pantalla()
        contingut=objectStorage.get_object('contenidortask1', 'resultat_finalAMQP.txt')
        contingut1=objectStorage.get_object('contenidortask1','countwordsAMQP.txt')
        fitx=open("resultat_final.txt","w+")
        fitx1=open("paraules_totals.txt","w+")
        res_string=contingut.decode("utf-8")
        res_string1=contingut1.decode("utf-8")
        fitx.write(res_string)
        fitx1.write(res_string1)
        fitx.close()
        fitx1.close()
        with ZipFile('resultats_OrchestratorCues_Cristina_Francesc.zip','w') as myzip:
            myzip.write('resultat_final.txt')
            myzip.write('paraules_totals.txt')
            path=myzip.printdir()
        ini_repositori_local('final')
        print("Generat zip amb resultats finals.")
        print("TEMPS amb: "+str(num_maps)+" wordcounts fent servir el fitxer: "+sys.argv[1]+" ha estat de: "+str(tf - ti)+" segons")
        channel.queue_delete('orchestrator')
        channel.queue_delete('hello')
        netejarObjectStorage(objectStorage)
        print("")
        print("Final del Orchestrator.")
        sys.exit()
    else:
        print ("Usage: Orchestrator.py <file_name> <num_maps>")