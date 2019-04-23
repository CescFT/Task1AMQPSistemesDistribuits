'''
Created on 24 de febr. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''

import sys, time, os,re, string
def esborrar_pantalla():
    os.system("cls")
    
def inicialitzar_carpeta():
    resfinaltxt='res_final_sequencial.txt'
    respartotalstxt='par_totals_sequencial.txt'
    if os.path.exists(resfinaltxt):
        os.remove(resfinaltxt)
    if os.path.exists(respartotalstxt):
        os.remove(respartotalstxt)
        
def countWords(merged):
    global temps_total
    ti=time.time()
    
    contador=0
    keys=merged.keys()
    for val in keys:
            contador+=merged[val]
    
    tf=time.time()
    dif=tf - ti
    temps_total+= dif
    return contador

def wordCount(text):
    global temps_total
    ti=time.time()
    diccionari={}
    words=""
    punctuation2 = "['-]"
    regex2 = r"(\s)"
    
    for value in text:
        if re.search(regex2, value):
            value = ' '
            words = words + value
        else:
            if not re.search(punctuation2, value) and re.search('['+string.punctuation+']', value):
                value = ' '
            words = words + value

    words = words.split(' ')
    for par in words:
        if par != "":
            if par in diccionari:
                diccionari[par]=diccionari.get(par)+1
            else:
                diccionari[par]=1
    tf=time.time()
    dif=tf-ti
    temps_total=temps_total + dif
    return diccionari
def merger(dicc2merge, final):
    global temps_total
    ti=time.time()
    for key in dicc2merge:
        if key in final:
            final[key]=final.get(key)+dicc2merge.get(key)
        else:
            final[key]=dicc2merge.get(key)
    tf=time.time()
    dif= tf - ti
    temps_total+=dif


if __name__ == '__main__':
    if len(sys.argv) == 2:
        esborrar_pantalla()
        inicialitzar_carpeta()
        print("Carpeta Inicialitzada:OK.")
        input("Presioni enter per a continuar...")
        esborrar_pantalla()
        print("Calculant els resultats: wordcount i countwords...")
        global temps_total
        temps_total=0
        f1=open(sys.argv[1], 'r')
        final={}
        nPar=0
        diccionari=wordCount(f1.read())
        merger(diccionari, final)
        nPar=countWords(final)
        par_totals='En total en el text hi ha '+str(nPar)+' paraules.'
        f2=open("res_final_sequencial.txt", "w+")
        f3=open("par_totals_sequencial.txt","w+") 
        f2.write(str(final))
        f3.write(par_totals)
        esborrar_pantalla()
        print("TEMPS total sequencial: "+str(temps_total)+ " segons. FITXER: "+sys.argv[1])
        print("Generats fitxers amb els resultats finals.")
        f1.close()
        f2.close()
        f3.close()
    else:
        print("Usage: AlgorismeSequencial.py <fitxer_a_llegir>")