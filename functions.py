#This file is intended to store custom functions to be utilized by the rule engine.
    #These functions can be utilized within the rule engine components, such as, decision tables, data attributes, and elsewhere.
    #These functions are imported whenever the rule engine is imported with the module name 'function' and can be called like a function from any other module
    #Example: function.length(string)
    
import re

def concat(dict_list, key):
    output = []
    for i in dict_list:
        output.append(i[key])
    return output

def length(string, pattern=''):
    return len(re.sub(pattern, '', string))

def add(string):
    return string + ' yo yo yo this yo boy'

def upper(string):
    return string.upper()
    
def cabin(string):
    return string[0]