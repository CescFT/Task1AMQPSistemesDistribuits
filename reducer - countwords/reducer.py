'''
Created on 25 de mar. 2019

@authors: Cristina Ionela Nistor i Francesc Ferre Tarres
'''
def reducer(args):
    llista_diccionaris = args.get("merge")
    res_final={}
    for dicc in llista_diccionaris:
        for key in dicc:
            if key in res_final:
                res_final[key]=res_final.get(key)+dicc.get(key)
            else:
                res_final[key]=dicc.get(key)
    return res_final