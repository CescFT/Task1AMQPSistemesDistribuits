'''
Created on 20 de mar. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''
import re
import string
def wordCount(args):
    dicc={}
    text = args.get("text")
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
            if par in dicc:
                dicc[par]=dicc.get(par)+1
            else:
                dicc[par]=1
    return dicc
        
        