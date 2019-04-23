'''
Created on 20 de mar. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''

def countWords(args):
    dicc = args.get("merge")
    contador=0
    keys=dicc.keys()
    for val in keys:
            contador+=dicc[val]
    
    return {"total words": contador}