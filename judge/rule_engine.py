# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 13:47:21 2020

@author: tjones727
"""
#for dataframe handling
import pandas as pd
#for file and directory handling
import os
#for string replacement
import re
#for apply rules
import operator
from functools import reduce
from ast import literal_eval
#for saving objects
import _pickle as pickle
#external functions for rule engine
from judge import functions


###############################################################################
#attribute class
###############################################################################

class attribute():
    """
    DESCRIPTION:
    This class creates 'attribute' objects utilized by the data structure class.
    It is used when extracting data from the input data.
    
    ATTRIBUTES:
    name (str; required) - The name of the attribute.
    
    attribute_path (list; required) - The path to the attribute in the input 
    data.
    
    dtype (type; required) - The datatype for the attribute.
    
    default (varies; optional; default:Nonetype) - The default value for the 
    attribute. Should match the attribute datatype.
    
    priority (int/float; optional; default:Nonetype) - The priority used when 
    extracting the attributes. The attribute with the lowest priority will be 
    applied first.
    
    function_dictionary (dict; optional; defualt:Nonetype) - A dictionary of 
    functions and parameters to apply transformations to the attribute once 
    extracted. The dictionary value should be the function itself. The value 
    should be a dictionary of parameters and their values. If there are no 
    parameters for the function a Nonetype value should be supplied.
        EXAMPLE (with no parameters): {functions.upper:None}
        EXAMPLE (with parameters supplied): {functions.concat: {'key':'price'}} 
    """
    ###########################################################################
    #initiate self
    ###########################################################################
    def __init__(self, name, attribute_path, dtype, default=None, priority=None,
                 function_dictionary=None):
        self._name = name
        self._attribute_path = attribute_path
        self._dtype = dtype
        self._default = default
        self._priority = priority
        self._function_dictionary = function_dictionary

###############################################################################
#expression class
###############################################################################

class expression():
    """
    DESCRIPTION:
    This class creates 'expression' objects utilized by the data structure 
    class. It is used to apply expressions to create new attributes following 
    attribute extraction.
    
    ATTRIBUTES:
    name (str; required) - The name of the expression.
    
    expression (str; requied) - The expression string to be executed.
    
    priority (int or float; optional; default:Nonetype) - The priority used 
    when applying expressions. The expression with the lowest priority will be 
    applied first.
    """
    ###########################################################################
    #initiate self
    ###########################################################################
    def __init__(self, name, expression, priority=None):
        self._name = name
        self._expression = expression
        self._priority = priority
        
###############################################################################
#data structure class
###############################################################################
class data_structure():
    """
    DESCRIPTION:
    This class creates 'data structure' objects utilized by the rule engine 
    class. The data structure class is comprised of attributes, expressions,
    and functions to add, remove, list, and extract the attributes and apply 
    the expressions on input data.
    
    ATTRIBUTES:
    attributes (list) - A list of attribute objects. 
    expressions (list) - A list of expression objects.
    """
    ###########################################################################
    #initiate self
    ###########################################################################
    def __init__(self):
        self._attributes = []
        self._expressions = []
    
    ###########################################################################
    #function to add/update attributes to object
    ###########################################################################
    
    def add_attribute(self, name, attribute_path, dtype, default=None, priority=None, function_dictionary=None):
        """
        DESCRIPTION:
        This function is used to add attributes to the data structure 
        object.
        
        PARAMETERS:
        name (str; required) - The name of the attribute to create.
        
        attribute_path (list; required) - The path to the attribute in the 
        input data specified as a list.
        
        dtype (type; required) -  The datatype for the attribute to create.
        
        default (varies; optional; default:Nonetype) - The default value to 
        return if the attribute is missing from the input data. The default 
        value should match the attribute datatype. 
        
        priority (int/float; optional; default:Nonetype) - The priority of the 
        attribute to create.
        
        function_dictionary (dict, optional; default:Nonetype) - The 
        function_dictionary of the attribute to create.
        
        OUTPUT/RESULT:
        The result of this function is a new attribute within the data 
        structure object.
        """
        try:
            if len(list(filter(lambda x: x._name == name, self._attributes))) == 0:
                self._attributes.append(attribute(name, attribute_path, dtype, default, priority, function_dictionary))
                print("Attribute '" + name + "' added to data structure")
            else:
                self._attributes = list(filter(lambda x: x._name != name, self._attributes))
                self._attributes.append(attribute(name, attribute_path, dtype, default, priority, function_dictionary))
                print("Attribute '" + name + "' already exists and will be replaced")
                print("Attribute '" + name + "' added to data structure")
        except Exception as e:
            print('ERROR: An error occured while adding attribute to data structure')
            print(e)  
            
    ###########################################################################
    #function to remove attributes from object
    ###########################################################################
    
    def remove_attribute(self, name):
        """
        DESCRIPTION:
        This function is used to remove attributes from the data structure 
        object.
        
        PARAMETERS:
        name (str; required) - The name of the attribute to remove from the
        data structure.
        
        OUTPUT/RESULT:
        The result of this function is the removal of the specified attribute 
        from the data structure object.
        """
        try:
            self._attributes = list(filter(lambda x: x._name != name, self._attributes))
            print("Attribute '" + name + "' removed from data structure")
        except Exception as e:
            print('ERROR: An error occured while removing attribute from data structure')
            print(e)  
    
    ###########################################################################
    #function to get attributes from object
    ###########################################################################
    
    def get_attributes(self, return_dataframe=False):
        """
        DESCRIPTION:
        This function is to print or return all attributes currently setup
        within the data structure object
        
        PARAMETERS:
        return_dataframe (bool; optional; default:False) - An option for 
        whether or not to return a pandas dataframe of attributes.
        
        OUTPUT/RESULT:
        The result of this function is a list of all attributes and their 
        attribute values which are configured within the data structure.
        """
        try:
            #function to format dictionary
            def format_function_dict(x):
                if x == None:
                    out = None
                else:
                    out = []
                    for function, parameters in x.items():
                        if x == None:
                            continue
                        else:
                            out.append({re.sub(' at.*>','',re.sub('<function ','',str(function))): str(parameters)})
                return out
            #output dictionary
            attribute_dict = {'name':[],
                              'attribute_path':[],
                              'dtype':[],
                              'default':[],
                              'priority':[],
                              'functions':[]}
            #loop through attributes and fill in dictionary
            for attribute in self._attributes:
                attribute_dict['name'].append(attribute._name)
                attribute_dict['attribute_path'].append('.'.join(attribute._attribute_path))
                attribute_dict['dtype'].append(attribute._dtype)
                attribute_dict['default'].append(attribute._default)
                attribute_dict['priority'].append(attribute._priority)
                attribute_dict['functions'].append(format_function_dict(attribute._function_dictionary))
            #return attributes as dataframe 
            if return_dataframe == True:
                return pd.DataFrame.from_dict(attribute_dict)
            else: 
                print(pd.DataFrame.from_dict(attribute_dict))
        except Exception as e:
            print('ERROR: An error occured while getting attributes')
            print(e)  
                
            
    ###########################################################################
    #function to add/update expressions to object
    ###########################################################################

    def add_expression(self, name, expression_string, priority=None, test_data=None, dataframe=False):
        """
        DESCRIPTION:
        This function is used to add expressions to the data structure 
        object.
        
        PARAMETERS:
        name (str; required) - The name of the expression to create.
        
        expression_string (str; required) - The expression string to execute.

        priority (int/float; optional; default:Nonetype) - The priority of the 
        expression to create.
        
        test_data (dataframe/dict, optional; default:Nonetype) - Test data to 
        ensure the expression being added is valid. The test data should be a 
        sample of the input data.The expression will only be added if it passes
        the validation. If no test_data is provided no vaildation will be 
        performed and the expression will be added.
        
        dataframe (bool; optional; default:False) - A parameter indicating if 
        the input data for the data structure is a dataframe. This parameter 
        will be inhereted from the rule engine object, but should be specified 
        if using the data structure object in a stand alone fashion.
        
        OUTPUT/RESULT:
        The result of this function is a new expression within the data 
        structure object.
        """
        if test_data is None:
            print("WARNING: No test data was provided and expression will not be validated")
            try:
                if len(list(filter(lambda x: x._name == name, self._expressions))) == 0:
                    self._expressions.append(expression(name, expression_string, priority))
                    print("Expression '" + name + "' added to data structure")
                else:
                    self._expressions = list(filter(lambda x: x._name != name, self._expressions))
                    self._expressions.append(expression(name, expression_string, priority))
                    print("Expression '" + name + "' already exists and will be replaced")
                    print("Expression '" + name + "' added to data structure")
            except Exception as e:
                print('ERROR: An error occured while adding expression to data structure')
                print(e)
        else:
            print('Validating expression on test_data')
            try:
                print('\tExtract attributes from test data')
                try:
                    test_data = self.extract_attributes(test_data, verbose=False, dataframe=dataframe)
                except Exception as e:
                    print('ERROR: An error occured while extracting attributes for test_data')
                    print(e)
                if dataframe == True:
                    print('\tTesting expression with dataframe...')
                    row_test = []
                    failed_rows = []
                    for row_index, row_data in test_data.items():
                        data = row_data
                        try:
                            exec(expression_string)
                            row_test.append(True)
                        except:
                            row_test.append(False)
                            failed_rows.append(str(row_index))
                            #print('\tExpression failed on data in row ' + str(row_index))
                    if len(test_data) == sum(row_test):
                        print('\tExpression valid on all dataframe rows and will be added to data structure')
                        try:
                            if len(list(filter(lambda x: x._name == name, self._expressions))) == 0:
                                self._expressions.append(expression(name, expression_string, priority))
                                print("Expression '" + name + "' added to data structure")
                            else:
                                self._expressions = list(filter(lambda x: x._name != name, self._expressions))
                                self._expressions.append(expression(name, expression_string, priority))
                                print("Expression '" + name + "' already exists and will be replaced")
                                print("Expression '" + name + "' added to data structure")
                        except Exception as e:
                            print('ERROR: An error occured while adding expression to data structure')
                            print(e)
                    else:
                        print("\tEXPRESSION INVALID: Expression failed on dataframe rows '" + ','.join(failed_rows) + "' and will not be added to data structure")
                else:
                    try:
                        print('\tTesting expression...')
                        data = test_data
                        exec(expression_string)
                        print('\tExpression valid')
                        try:
                            if len(list(filter(lambda x: x._name == name, self._expressions))) == 0:
                                self._expressions.append(expression(name, expression_string, priority))
                                print("Expression '" + name + "' added to data structure")
                            else:
                                self._expressions = list(filter(lambda x: x._name != name, self._expressions))
                                self._expressions.append(expression(name, expression_string, priority))
                                print("Expression '" + name + "' already exists and will be replaced")
                                print("Expression '" + name + "' added to data structure")
                        except Exception as e:
                            print('ERROR: An error occured while adding expression to data structure')
                            print(e)
                    except Exception as e:
                        print('\tEXPRESSION INVALID: Expression will not be added to data structure')
                        print('\tERROR: ' + str(type(e)) + ' - ' + str(e)) 
            except Exception as e:
                print('ERROR: An error occured while testing expression on test_data')
                print(e)


    ###########################################################################
    #function to remove expression from object
    ###########################################################################
    
    def remove_expression(self, name):
        """
        DESCRIPTION:
        This function is used to remove expressions from the data structure 
        object.
        
        PARAMETERS:
        name (str; required) - The name of the expression to remove from the
        data structure.
        
        OUTPUT/RESULT:
        The result of this function is the removal of the specified expression 
        from the data structure object.
        """
        try:
            self._expressions = list(filter(lambda x: x._name != name, self._expressions))
            print("Expression '" + name + "' removed from data structure")
        except Exception as e:
            print('ERROR: An error occured while removing expression from data structure')
            print(e)  
            
    ###########################################################################
    #function to get expressions from object
    ###########################################################################
    
    def get_expressions(self, return_dataframe=False):
        """
        DESCRIPTION:
        This function is to print or return all expressions currently setup
        within the data structure object
        
        PARAMETERS:
        return_dataframe (bool; optional; default:False) - An option for 
        whether or not to return a pandas dataframe of expressions.
        
        OUTPUT/RESULT:
        The result of this function is a list of all expressions and their 
        attribute values which are configured within the data structure.
        """
        try:
            #output dictionary
            expression_dict = {'name':[],
                              'expression':[],
                              'priority':[]}
            #loop through expressions and fill in dictionary
            for expression in self._expressions:
                expression_dict['name'].append(expression._name)
                expression_dict['expression'].append(expression._expression)
                expression_dict['priority'].append(expression._priority)
            #return expressions as dataframe 
            if return_dataframe == True:
                return pd.DataFrame.from_dict(expression_dict)
            else: 
                print(pd.DataFrame.from_dict(expression_dict))
        except Exception as e:
            print('ERROR: An error occured while getting expressions')
            print(e)  
        
    ###########################################################################
    #function to apply functions to attributes
    ##########################################################################
    def apply_functions(self, attribute, function_dictionary, verbose=True):
        """
        DESCRIPTION:
        This function is used to apply the functions configured within the
        attribute object.
        
        PARAMETERS:
        attribute (varies; required) - The extracted attribute from input data.
        
        function_dictionary (dict; required) - A dictionary of functions to
        apply.
        
        verbose (bool; optional; default:True) - Whether or not to print 
        details when applying functions. Primarily used for testing and 
        validation. This will be inherited from the rule engine object.
        
        OUTPUT/RESULT:
        The result of this function is the execution of all functions 
        configured with this function dictionary of the data structure 
        attribute.
        """
        try:
            output = attribute
            for function, parameters in function_dictionary.items():
                if parameters == None:
                    print_verbose(verbose, "\tApplying function '" + str(function) + "' without parameters")
                    print_verbose(verbose, '\t\tInput: ' + str(output))
                    output = function(output)
                    print_verbose(verbose, '\t\tOutput: ' + str(output))
                else: 
                    print_verbose(verbose, "\tApplying function '" + str(function) + "' with parameter dictionary " + str(parameters))
                    print_verbose(verbose, '\t\tInput: ' + str(output))
                    output = function(output, **literal_eval(str(parameters)))
                    print_verbose(verbose, '\t\tOutput: ' + str(output))
            return output
        except Exception as e:
            print('ERROR: An error occured while applying functions to attribute')
            print(e)
    
    ###########################################################################
    #function to extract attributes
    ###########################################################################  

    def extract_attributes(self, data, verbose=True, dataframe=False):
        """
        DESCRIPTION:
        This function is used extract all attributes and apply all expressions
        configured within the data structure object.
        
        PARAMETERS:
        data (dict/dataframe; required) - Input data to extract attributes 
        from.
        
        verbose (bool; optional; default:True) - Whether or not to print 
        details when applying function. Primarily used for testing and 
        validation. This  will be inherited from the rule engine object.
        
        dataframe (bool; optional; default: False) - A parameter indicating if 
        the input data for the data structure is a dataframe. This parameter 
        will be inhereted from the rule engine object, but should be specified 
        if using the data structure object in a stand alone fashion.
        
        OUTPUT/RESULT:
        The result of this function is the execution of all functions 
        configured with this function dictionary of the data structure 
        attribute.
        """
        try:
            #create empty dictionary to fill in
            output_dict = {}
            #sort attributes by priority
            sorted_attributes = sorted(self._attributes, key=lambda x: (x._priority is None, x._priority))
            print_verbose(verbose, "Extracting attributes configured in data structure")
            #logic for when data structure is for pandas dataframe
            if dataframe == True:
                #transpose and convert dataframe to dictionary
                df_dict = data.where(pd.notnull(data), None).transpose().to_dict()
                #loop through df dictionary to extract attributes
                for row_index, row_data in df_dict.items():
                    #create empty dictionary to store output from individual rows
                    row_dict = {}
                    #loop through attribute dictionary extract and transform attributes
                    for attribute in sorted_attributes:
                        #logic if function is not included
                        if attribute._function_dictionary == None:
                            print_verbose(verbose, "\tExtracting attribute '" + attribute._name + "' from row " + str(row_index) + " without applying functions")
                            try:
                                if reduce(operator.getitem, attribute._attribute_path, row_data) == None:
                                    print_verbose(verbose, "\t\tAttribute value missing, applying default value '" + str(attribute._default) + "'")
                                    row_dict[attribute._name] = attribute._dtype(attribute._default) if attribute._default!= None else None
                                else:
                                    row_dict[attribute._name] = attribute._dtype(reduce(operator.getitem, attribute._attribute_path, row_data))
                            except KeyError:
                                print_verbose(verbose, "\t\tAttribute '" + attribute._name + "' not in dataframe at column '" + str(attribute._attribute_path) + "', applying default value '" + str(attribute._default) + "'")
                                row_dict[attribute._name] = attribute._dtype(attribute._default) if attribute._default != None else None
                        #logic if functions are included
                        else:
                            print_verbose(verbose, "\tExtracting attribute '" + attribute._name + "' from row " + str(row_index) + "' and applying functions")
                            try:
                                if reduce(operator.getitem, attribute._attribute_path, row_data) == None:
                                    print_verbose(verbose, "\t\tAttribute value missing, applying default value '" + str(attribute._default) + "' without applying functions")
                                    row_dict[attribute._name] = attribute._dtype(attribute._default) if attribute._default != None else None
                                else:            
                                    row_dict[attribute._name] = attribute._dtype(self.apply_functions(reduce(operator.getitem, attribute._attribute_path, row_data), attribute._function_dictionary, verbose))
                            except KeyError:
                                print_verbose(verbose, "\t\tAttribute '" + attribute._name + "' not in dataframe at column '" + str(attribute._attribute_path) + "', applying default value '" + str(attribute._default) + "' without applying functions")
                                row_dict[attribute._name] = attribute._dtype(attribute._default) if attribute._default != None else None
                    #execute expressions for reach row
                    if len(self._expressions) > 0:
                        print_verbose(verbose, "Executing expressions configured in data structure for row " + str(row_index))
                        sorted_expressions = sorted(self._expressions, key=lambda x: (x._priority is None, x._priority))
                        data = row_dict
                        for expression in sorted_expressions:
                            try:
                                print_verbose(verbose, "\tExecuting expression '" + expression._name + "'")
                                exec(expression._expression)
                            except Exception as e:
                                print("ERROR: An error occured while executing expression '" + expression._name + "'")
                                print(e)
                        output_dict[row_index] = data
                    else:
                        output_dict[row_index] = row_dict
            #logic when data structure is not a dataframe
            else:
                #loop through attribute dictionary extract and transform attributes
                for attribute in sorted_attributes:
                    #logic if function is not included
                    if attribute._function_dictionary == None:
                        print_verbose(verbose, "\tExtracting attribute '" + attribute._name + "' without applying functions")
                        try:
                            output_dict[attribute._name] = attribute._dtype(reduce(operator.getitem, attribute._attribute_path, data))
                        except Exception as e:
                            print_verbose(verbose, "\t\tAn Error occured while extracting attribute '" + attribute._name + "' at path '" + '.'.join(attribute._attribute_path) + "', applying default value '" + str(attribute._default) + "'")
                            print_verbose(verbose, "\t\tERROR: '" + str(type(e)) + ' - ' + str(e))
                            output_dict[attribute._name] = attribute._dtype(attribute._default) if attribute._default != None else None
                    #logic if functions are includeed
                    else:
                        print_verbose(verbose, "\tExtracting attribute '" + attribute._name + "' and applying functions...")
                        try:
                            output_dict[attribute._name] =  attribute._dtype(self.apply_functions(reduce(operator.getitem, attribute._attribute_path, data), attribute._function_dictionary, verbose))
                        except Exception as e:
                            print_verbose(verbose, "\t\tAn Error occured while extracting attribute '" + attribute._name + "' at path '" + '.'.join(attribute._attribute_path) + "', applying default value '" + str(attribute._default) + "'")
                            print_verbose(verbose, "\t\tERROR: '" + str(type(e)) + ' - ' + str(e))
                            output_dict[attribute._name] = attribute._dtype(attribute._default) if attribute._default != None else None
                #logic to execute expressions if they exist
                if len(self._expressions) > 0:
                    print_verbose(verbose, "Executing expressions configured in data structure")
                    sorted_expressions = sorted(self._expressions, key=lambda x: (x._priority is None, x._priority))
                    data = output_dict
                    for expression in sorted_expressions:
                        try:
                            print_verbose(verbose, "\tExecuting expression '" + expression._name + "'")
                            exec(expression._expression)
                        except Exception as e:
                            print("ERROR: An error occured while executing expression '" + expression._name + "'")
                            print(e)
                    output_dict = data
            #return output
            return output_dict
        except Exception as e:
            print('ERROR: An error occured while extracting attributes from data or applying expressions')
            print(e)  
    
###############################################################################
#collection class
###############################################################################

#placeholder for collection class
class collection():
    """
    DESCRIPTION:
    This class creates 'collection' objects utilized by the rule engine 
    class. The collection class is comprised of attribute and functions 
    manage collections of decision tables and the configurations which control
    them.
    
    ATTRIBUTES:
    name (str; required) - The name of the collection.
    
    directory (str; required) - The directory for the collection to store the
    configuration table.
    
    active (bool; required; default:True) - Attribute indicating whether or not
    the collection is active.
    
    priority (int/float; optional) - The priority used when applying 
    collections. The collection with the lowest priority will be applied first.
    
    configuration(dict, optional; default:Nonetype) - The configuration the
    collection uses when executing decision tables. Although this says optional
    this is required to execute a collection.
    """
    ###########################################################################
    #initiate self
    ###########################################################################
    
    def __init__(self, name, directory, priority, active=True):
        self._name = name
        self._directory = directory
        self._active = active
        self._priority = priority
        self._configuration = None
    
    ###########################################################################
    #function for creating configuration tables for collection
    ###########################################################################
    
    def create_config_table(directory):
        """
        DESCRIPTION:
        This function is used to create a configuration table when a collection
        is created.
        
        PARAMETERS:
        directory (str; required) - The directory to create the configuration
        table in.
        
        OUTPUT/RESULT:
        The result of this function is a csv file with all fields required for
        a collection's configuration.
        """
        try:
            config_table = pd.DataFrame({'Decision Table Filename': [],
                            'Priority': [],
                            'Active': [],
                            'Execute All Rules': [],
                            'Score Override': [],
                            'Manual Flag': []
                            })
            config_table.to_csv(directory + 'configuration_table.csv', index = False)
            print(directory + 'configuration_table.csv created')
        except Exception as e:
            print('ERROR: An error occured while creating configuration table')
            print(e)

    ###########################################################################
    #function for importing configuration tables for collection
    ###########################################################################
    
    def import_config_table(directory):
        """
        DESCRIPTION:
        This function is used to import a configuration table for a collection
        directory.
        
        PARAMETERS:
        directory (str; required) - The directory to import the configuration
        table from.
        
        OUTPUT/RESULT:
        The result of this function is a dictionary of configurations for the
        collection.
        """
        try:
            config_table = pd.read_csv(directory + 'configuration_table.csv', 
                                       index_col = 'Decision Table Filename').sort_values('Priority')
            config_table = config_table.where(pd.notnull(config_table), None).to_dict(orient = 'index')
            print(directory + 'configuration_table.csv imported')
            return config_table
        except Exception as e:
            print('ERROR: An error occured while importing configuration table')
            print(e)  

    ###########################################################################
    #function to apply decision tables in collection
    ###########################################################################
            
    def apply_collection(self, data, decision_table_dictionary, verbose=True):
        """
        DESCRIPTION:
        This function is used to apply all the decision tables configured 
        within the collection.
        
        PARAMETERS:
        data (dict; required) - Input data.
        
        decision_table_dictionary (dict; required) - The decision table to
        execute.
        
        verbose (bool; optional; default:True) - Whether or not to print 
        details when applying collection. Primarily used for testing and 
        validation. This  will be inherited from the rule engine object.
        
        OUTPUT/RESULT:
        The result of this function is a dictionary containing the results of 
        the executed collection.
        """
        dt_results = {}
        final_result = {'collection_result':None,
                        'collection_score':None,
                        'collection_tables':None,
                        'collection_hits':None,
                        'rule_hits':None,
                        'audit_trail':{'decision_tables':[],
                                      'flags':[],
                                      'scores':[],
                                      'rules':[],
                                      'rule_matches':[],
                                      'rule_scores':[]
                                      }}
        counter = 0
        for dt_name in self._configuration:
            print_verbose(verbose,"Executing decision table '" + dt_name + "'")
            #add to counter
            counter = counter + 1
            if self._configuration[dt_name]['Active'] == True:
                #set configuration params
                run_all = self._configuration[dt_name]['Execute All Rules']
                manual_flag = self._configuration[dt_name]['Manual Flag']
                score_override = self._configuration[dt_name]['Score Override']
                #apply decision table to data and save results
                result = apply_decision_table(decision_table_dictionary[dt_name], data, run_all, verbose)
                if result['decision_table_result'] == True:
                    dt_results[dt_name] = result
                    final_result['audit_trail']['decision_tables'].append(dt_name)
                    final_result['audit_trail']['flags'].append(manual_flag)
                    final_result['audit_trail']['rules'].extend(result['audit_trail']['rules'])
                    final_result['audit_trail']['rule_matches'].extend(result['audit_trail']['match'])
                    final_result['audit_trail']['rule_scores'].extend(result['audit_trail']['score'])
                    final_result['audit_trail']['scores'].append(score_override if score_override != None else result['decision_table_score'])
            else:
                print_verbose(verbose,"Decision table '" + dt_name + "' is not active, continue to next decision table")
                continue
        final_result['collection_result'] = True if len(final_result['audit_trail']['decision_tables']) > 0 else False
        final_result['collection_score'] =  sum(final_result['audit_trail']['scores'])
        final_result['collection_tables'] = counter
        final_result['collection_hits'] = len(final_result['audit_trail']['decision_tables'])
        final_result['rule_hits'] = len(final_result['audit_trail']['rules'])
        return final_result

###############################################################################
#rule engine class
###############################################################################

class rule_engine():
    """
    DESCRIPTION:
    This class creates the primary 'rule_engine' object. The rule engine 
    coordinates the execution of the data structure, collection, and rule 
    engine components on input data.
    
    ATTRIBUTES:
    
    main_directory (str; required) - The main directory the rule engine and
    associated folders and files will be located in.
    
    name (str; optional; default:Nonetype) - The name of the rule engine.
    
    description (str; required) - A description for the rule engine.
    
    dataframe (bool; optional; default:False) - An attribute indicating whether
    or not the input data will be a dataframe.
    
    verbose (bool; optional; default:True) - Whether or not to print 
    details when executing rule engine. Primarily used for testing and 
    validation.
    
    data_structure (object; required) - The data structure object associated
    with the rule engine. Will be added upon creation.
    
    decision_table_dict (dict; required) - A dictionary of decision tables
    associated with the rule engine.
    
    collections (list, required) - A list of collection objects associated with
    the rule engine.
    """
    ###########################################################################
    #initiate self
    ###########################################################################
    
    def __init__(self, main_directory, name=None, description=None, dataframe=False):
        self._main_directory = main_directory
        self._name = name
        self._description = description
        self._dataframe = dataframe
        self._verbose = True
        self._data_structure = data_structure()
        self._decision_table_dict = None
        self._collections = []
            
    ###########################################################################
    #function to add/update attributes to object
    ###########################################################################
    
    def add_attribute(self, name, attribute_path, dtype, default=None, priority=None, function_dictionary=None):
        """
        DESCRIPTION:
        This function is used to add attributes to the data structure 
        object within the rule engine.
        
        PARAMETERS:
        name (str; required) - The name of the attribute to create.
        
        attribute_path (list; required) - The path to the attribute in the 
        input data specified as a list.
        
        dtype (type; required) -  The datatype for the attribute to create.
        
        default (varies; optional; default:Nonetype) - The default value to 
        return if the attribute is missing from the input data. The default 
        value should match the attribute datatype. 
        
        priority (int/float; optional; default:Nonetype) - The priority of the 
        attribute to create.
        
        function_dictionary (dict, optional; default:Nonetype) - The 
        function_dictionary of the attribute to create.
        
        OUTPUT/RESULT:
        The result of this function is a new attribute within the data 
        structure object of the rule engine.
        """
        self._data_structure.add_attribute(name, attribute_path, dtype, default, priority, function_dictionary)
    
    ###########################################################################
    #function to remove attributes from object
    ###########################################################################
    
    def remove_attribute(self, name):
        """
        DESCRIPTION:
        This function is used to remove attributes from the data structure 
        object within the rule engind.
        
        PARAMETERS:
        name (str; required) - The name of the attribute to remove from the
        data structure.
        
        OUTPUT/RESULT:
        The result of this function is the removal of the specified attribute 
        from the data structure object within the rule engine.
        """
        self._data_structure.remove_attribute(name)
            
    ###########################################################################
    #function to get attributes
    def get_attributes(self, return_dataframe=False):
        """
        DESCRIPTION:
        This function is to print or return all attributes currently setup
        within the data structure object within the rule engine.
        
        PARAMETERS:
        return_dataframe (bool; optional; default:False) - An option for 
        whether or not to return a pandas dataframe of attributes.
        
        OUTPUT/RESULT:
        The result of this function is a list of all attributes and their 
        attribute values which are configured within the data structure within 
        the rule engine.
        """
        return self._data_structure.get_attributes(return_dataframe)
            
    ###########################################################################
    #function to add/update expressions to object
    ###########################################################################

    def add_expression(self, name, expression_string, priority=None, test_data=None):
        """
        DESCRIPTION:
        This function is used to add expressions to the data structure 
        object with the rule engine.
        
        PARAMETERS:
        name (str; required) - The name of the expression to create.
        
        expression_string (str; required) - The expression string to execute.

        priority (int/float; optional; default:Nonetype) - The priority of the 
        expression to create.
        
        test_data (dataframe/dict, optional; default:Nonetype) - Test data to 
        ensure the expression being added is valid. The test data should be a 
        sample of the input data.The expression will only be added if it passes
        the validation. If no test_data is provided no vaildation will be 
        performed and the expression will be added.
        
        dataframe (bool; optional; default:False) - A parameter indicating if 
        the input data for the data structure is a dataframe. This parameter 
        will be inhereted from the rule engine object, but should be specified 
        if using the data structure object in a stand alone fashion.
        
        OUTPUT/RESULT:
        The result of this function is a new expression within the data 
        structure object within the rule engine.
        """
        self._data_structure.add_expression(name, expression_string, priority=priority, test_data=test_data, dataframe=self._dataframe)

    ###########################################################################
    #function to remove expression from object
    ###########################################################################
    
    def remove_expression(self, name):
        """
        DESCRIPTION:
        This function is used to remove expressions from the data structure 
        object within the rule engine.
        
        PARAMETERS:
        name (str; required) - The name of the expression to remove from the
        data structure.
        
        OUTPUT/RESULT:
        The result of this function is the removal of the specified expression 
        from the data structure object within the rule engine.
        """
        self._data_structure.remove_expression(name)
    
    ###########################################################################
    #function to print attributes from data structure object
    ###########################################################################
    
    def get_expressions(self, return_dataframe=False):
        """
        DESCRIPTION:
        This function is to print or return all expressions currently setup
        within the data structure object of the rule engine.
        
        PARAMETERS:
        return_dataframe (bool; optional; default:False) - An option for 
        whether or not to return a pandas dataframe of expressions.
        
        OUTPUT/RESULT:
        The result of this function is a list of all expressions and their 
        attribute values which are configured within the data structure rule
        engine.
        """
        return self._data_structure.get_expressions(return_dataframe)
        
    ###########################################################################
    #function to import decision tables
    ###########################################################################
    
    def import_decision_tables(self):
        """
        DESCRIPTION:
        This function imports decision tables in the 'decision_tables' 
        directory created by the rule engine.
        
        OUTPUT/RESULT:
        The decision tables are imported and added to the 'decision_table_dict'
        rule engine attribute.
        """
        try:
            #create directory
            directory = self._main_directory + '/decision_tables/'
            #list items in directory
            directory_items = os.listdir(directory)
            #create empty dictionary to store results
            decision_table_dict = {}
            #open files and store in dictionary
            for i in directory_items:
                if os.path.isfile(directory + i):
                    print_verbose(self._verbose, "Importing decision table '" + i + "'")
                    decision_table_dict[i] = pd.read_csv(directory + i)
            #add dictionary to rule engine object
            self._decision_table_dict = decision_table_dict
            print('Decision table import successful, '
                  + str(len(decision_table_dict))
                  + ' decision tables imported and added to rule engine')
        except Exception as e:
            print('ERROR: An error occured while importing decision tables')
            print(e)  
    
    ###########################################################################
    #function to get decision tables
    ###########################################################################

    def get_decision_tables(self, return_dataframe=False):
        """
        DESCRIPTION:
        This function is to print or return all decision tables currently 
        within the 'decision_table_dict' dictionary.
        
        PARAMETERS:
        return_dataframe (bool; optional; default:False) - An option for 
        whether or not to return a pandas dataframe of decision tables.
        
        OUTPUT/RESULT:
        The result of this function is a list of all decision tables within the
        rule engine.
        """
        try:
            tables = {'decision_tables':[]}
            for table in self._decision_table_dict:
                tables['decision_tables'].append(table)
            if return_dataframe == True:
                return pd.DataFrame.from_dict(tables)
            else:
                print(pd.DataFrame.from_dict(tables))
        except Exception as e:
            print('ERROR: An error occured while getting dataframes')
            print(e)  
            
    ###########################################################################
    #function to add collection
    ###########################################################################
    
    def add_collection(self, name, priority, active=True):
        """
        DESCRIPTION:
        This function is used to create and add collection objects to the 
        collections list of the rule engine object.
        
        PARAMETERS:
        name (str; required) - The name of the collection to create.

        priority (int/float; optional) - The priority of the collection to 
        create.
        
        active (bool; required; default:True) : Parameter to determine whether 
        or not the collection should be active.
        
        OUTPUT/RESULT:
        The result of this function is a new collection added to the 
        'collections' attribute of the rule enngine object as well as a new
        sub-directory and configuration file for the collection. 
        """
        try:
            #create seperate directory for collection
            directory = self._main_directory + '/collections/' + name + '/'
            if name not in os.listdir(self._main_directory + '/collections/'):
                os.mkdir(directory)
                #add configuration table to directory
                collection.create_config_table(directory)
                #create collection object
                obj = collection(name, directory, priority, active)
                self._collections.append(obj)
                print("Collection added to rule engine and directory '" + name + "' created, next steps...")
                print('1 - Populate configuration file')
                print('2 - Import configurations')
            else:
                print('ATTENTION: Collection directory already exists. Directory and configuration table will not be created.')
                #create collection object
                obj = collection(name, directory, priority, active)
                self._collections.append(obj)
                print('Collection added to rule engine, next steps...')
                print('1 - Populate configuration file')
                print('2 - Import configurations')
        except Exception as e:
            print('ERROR: An error occured while adding collection to rule engine')
            print(e)  
    
    ###########################################################################
    #function to remove collection
    ###########################################################################
    
    def remove_collection(self, name, remove_directory=False):
        """
        DESCRIPTION:
        This function is used to remove a collection object from the
        collections list of the rule engine object and the directory for the
        collection if desired.
        
        PARAMETERS:
        name (str; required) - The name of the collection to remove.

        remove_directory (bool; required; default:False) - Parameter to 
        determine whether or not the directory and configuraion file for 
        collection should be removed.
        
        OUTPUT/RESULT:
        The result of this function will be the removal of the specified
        collection and its directory and configuration file if desired.
        """
        try:
            if len(self._collections) == 0:
                print('ATTENTION: There are no collections in rule engine')
            else:
                #create seperate directory for collection
                for collection in self._collections:
                    if collection._name == name:
                        self._collections.remove(collection)
                        print("Collection '" + name + "' removed from rule engine collections")
                        if remove_directory == True:
                            try:
                                os.remove(collection._directory + 'configuration_table.csv')
                                print("Configuration table removed for collection '" + name + "'")
                                os.rmdir(collection._directory)
                                print("Directory removed for collection '" + name + "'")
                            except Exception as e:
                                print('ERROR: An error occured while removing directory for collection')
                                print(e) 
                    else:
                        print("Collection '" + name + "' not found in rule engine")
        except Exception as e:
            print('ERROR: An error occured while removing collection from rule engine')
            print(e) 
            
    ###########################################################################
    #function to get collections for rule engine
    ###########################################################################
    
    def get_collections(self, return_dataframe=False):
        """
        DESCRIPTION:
        This function is to print or return all collections and its attributes
        currently within the 'collections' list of the rule engine object.
        
        PARAMETERS:
        return_dataframe (bool; optional; default:False) - An option for 
        whether or not to return a pandas dataframe of collections and their
        attributes.
        
        OUTPUT/RESULT:
        The result of this function is a list of all collections within the
        rule engine object.
        """
        try:
            coll_dict = {'name':[], 'active':[], 'priority':[], 'directory':[]}
            for collection in self._collections:
                coll_dict['name'].append(collection._name)
                coll_dict['active'].append(collection._active)
                coll_dict['priority'].append(collection._priority)
                coll_dict['directory'].append(collection._directory)
            if return_dataframe == True:
                return pd.DataFrame.from_dict(coll_dict)
            else:
                print(pd.DataFrame.from_dict(coll_dict))
        except Exception as e:
            print('ERROR: An error occured while getting collections')
            print(e)  
    
    ###########################################################################
    #function to import configurations for collections
    ###########################################################################
    
    def import_configurations(self):
        """
        DESCRIPTION:
        This function is to import the configuration tables for all
        collections within the 'collections' list attribute of the rule engine
        object. 
        
        OUTPUT/RESULT:
        The result of this function will update the configuration for all
        collections setup within the rule engine.
        """
        try:
            for i in self._collections:
                i._configuration = collection.import_config_table(i._directory)
                print_verbose(self._verbose,"Configuration imported for collecion '" + i._name + "'")
            print('Configurations for all collections imported to rule engine')
        except Exception as e:
            print('ERROR: An error occured while importing configurations for collections')
            print(e)
            
    ###########################################################################
    #function to run rule engine on data
    ###########################################################################
    
    def run(self, data):
        """
        DESCRIPTION:
        This function is run the rule engine on a set of data.
        
        PARAMETERS:
        data (dict/dataframe; required) - Input data to pass to the rule 
        engine.
        
        OUTPUT/RESULT:
        The result of this function will be the output generated by the rule 
        engine.
        """
        try:
            #extract attributes from data
            data = self._data_structure.extract_attributes(data, self._verbose, self._dataframe)
            #sort collections list by priority
            collections_sorted = sorted(self._collections, key=lambda x: x._priority)
            #set output dictionary
            output = {}
            if self._dataframe == True:
                for row, value in data.items():
                    df_output = {}
                    for collection in collections_sorted:
                        if collection._active == False:
                            print_verbose(self._verbose,"Collection '" + collection._name + "' is not active, continue to next collection")
                            continue
                        else:
                            print_verbose(self._verbose,"Executing collection '" + collection._name + "'")
                            df_output[collection._name] = collection.apply_collection(data=value, 
                                  decision_table_dictionary = self._decision_table_dict,
                                  verbose = self._verbose)
                            output[row] = df_output
                output = output_to_df(output, self._dataframe)
            else:
                for collection in collections_sorted:
                    if collection._active == False:
                        print_verbose(self._verbose,"Collection '" + collection._name + "' is not active, continue to next collection")
                        continue
                    else:
                        print_verbose(self._verbose,"Executing collection '" + collection._name + "'")
                        output[collection._name] = collection.apply_collection(data=data, 
                              decision_table_dictionary = self._decision_table_dict,
                              verbose = self._verbose)
            return output
        except Exception as e:
            print('ERROR: An error occured while running rule engine')
            print(e)
    

    ###########################################################################
    #function to create decision table template
    ###########################################################################
    def create_decision_table_template(self):
        """
        DESCRIPTION:
        This function is to create a template for decision tables based on the
        attributes within the rule engine.
        
        OUTPUT/RESULT:
        An excel file template for creating decision tables.
        """
        try:
            #create dataframe for available attributes
            attributes = self.get_attributes(True)
            
            #create dataframe for available expressions
            expressions = self.get_expressions(True)
            
            #create dataframe for decision table template
            template = pd.DataFrame({'attribute_name_1':['condition logic (e.g. X > 0)', None], 'attribute_name_2':[None, "condition logic (e.g. X == 'foo')"], 'Rule Name':[None, None], 'Rule Score':[None, None]})
            
            #create dataframe for steps
            steps = pd.DataFrame({'Step':[1,2,3,4,5],
                              'Action':["Add desired attribute names to columns in front of 'Rule Name' and 'Rule Score' columns. Do NOT remove 'Rule Name' and 'Rule Score' columns as they are required.",
                                        "Add conditional logic under desired attribute column. The value 'X' will represent the attribute value.",
                                        "Add name of the rule to 'Rule Name' column. This is suggested for auditting rules, but not required. A value of None will be returned if Rule Name value is not added.",
                                        "Add score to be applied for rule in 'Rule Score' column. A value of 0 will be returned if Rule Score value is not added",
                                        "Once decision table is completed, save decision table in '" + self._main_directory + "/decision_tables' folder as csv file."]})
                              
            #write dataframes to excel workbook
            filename = self._main_directory + "/decision_table_template.xlsx"
            with pd.ExcelWriter(filename) as writer:  
                template.to_excel(writer, sheet_name='decision_table_template', index = False)
                attributes.to_excel(writer, sheet_name='available_attributes', index = False)
                expressions.to_excel(writer, sheet_name='applied_expressions', index = False)
                steps.to_excel(writer, sheet_name='steps', index = False)
            print("Decision table template created at '" + filename + "'")
        except Exception as e:
            print('ERROR: An error occured while creating decision table template')
            print(e)

###########################################################################
#function to convert output to dataframe
###########################################################################

def output_to_df(output, dataframe):
    """
    DESCRIPTION:
    This function is to convert the output of the rule engine to a dataframe.
    
    PARAMETERS:
    output (dict; required) - The output of the rule engine.
    
    dataframe (bool; required) - A parameter for whether or not the input data
    is a dataframe
    
    OUTPUT/RESULT:
    The result of this function will be the rule engine output as a pandas
    dataframe object.
    """
    out_dict = {}
    if dataframe == True:
        for row, value in output.items():
            coll_dict = {}
            for collection, result in value.items():
                coll_dict[collection + '_result'] = result['collection_result']
                coll_dict[collection + '_score'] = result['collection_score']
                coll_dict[collection + '_tables'] = result['collection_tables']
                coll_dict[collection + '_hits'] = result['collection_hits']
                coll_dict[collection + '_rule_hits'] = result['rule_hits']
                coll_dict[collection + '_tables'] = str(result['audit_trail']['decision_tables'])
                coll_dict[collection + '_table_flags'] = str(result['audit_trail']['flags'])
                coll_dict[collection + '_table_scores'] = str(result['audit_trail']['scores'])
                coll_dict[collection + '_rules'] = str(result['audit_trail']['rules'])
                coll_dict[collection + '_rule_matches'] = str(result['audit_trail']['rule_matches'])
                coll_dict[collection + '_rule_scores'] = str(result['audit_trail']['rule_scores'])
                out_dict[row] = coll_dict
    else:
        coll_dict = {}
        for collection, result in output.items():
            coll_dict[collection + '_result'] = result['collection_result']
            coll_dict[collection + '_score'] = result['collection_score']
            coll_dict[collection + '_tables'] = result['collection_tables']
            coll_dict[collection + '_hits'] = result['collection_hits']
            coll_dict[collection + '_rule_hits'] = result['rule_hits']
            coll_dict[collection + '_tables'] = str(result['audit_trail']['decision_tables'])
            coll_dict[collection + '_table_flags'] = str(result['audit_trail']['flags'])
            coll_dict[collection + '_table_scores'] = str(result['audit_trail']['scores'])
            coll_dict[collection + '_rules'] = str(result['audit_trail']['rules'])
            coll_dict[collection + '_rule_matches'] = str(result['audit_trail']['rule_matches'])
            coll_dict[collection + '_rule_scores'] = str(result['audit_trail']['rule_scores'])
        out_dict[0] = coll_dict
    return  pd.DataFrame.from_dict(out_dict, orient='index')

###########################################################################
#function to apply single decision table
###########################################################################



def apply_decision_table(decision_table, data, run_all=False, verbose=True):
    """
    DESCRIPTION:
    This function is to apply a decision tablee to input data.
    
    PARAMETERS:
    decision_table (dataframe; required) - A decision table as a dataframe.
    
    data (dict; required) - Input data for the decision table.
    
    run_all (bool, optional; default:False) - A parameter for whether or not
    to evaluate all rows within the decision table. Default behavior is to
    exit the decision table when the first row evaluates to True.
    
    verbose (bool; optional; default:True) - Whether or not to print 
    details when applying decision table. Primarily used for testing and 
    validation.
    
    OUTPUT/RESULT:
    The result of this function will be the result of the decision table.
    """
    try:
        #replace na values in decision_table
        dt = decision_table.where(pd.notnull(decision_table), None).transpose().to_dict()
        #create empty dictionary for outputs
        output_dict = {'decision_table_result':None, 
                       'decision_table_score':None, 
                       'decision_table_hits':None,
                       'audit_trail': {'rules':[],
                                       'match':[],
                                       'score':[]}}
        #create counter variable to track cells with condition
        for rule in range(len(dt)):
            rule_name = dt[rule]['Rule Name']
            rule_score = 0 if dt[rule]['Rule Score'] == None else dt[rule]['Rule Score']
            conditions = 0
            hit_list = []
            print_verbose(verbose,'\tEvaluate rule: ' + str(rule_name))
            for column, condition in dt[rule].items():
                if column not in ['Rule Name', 'Rule Score'] and condition != None:
                    conditions = conditions + 1
                    X = data[column]
                    output = bool(eval(condition))
                    print_verbose(verbose,'\t\tColumn: ' + str(column) + '; Value:' + str(X) + '; Condition: ' + str(condition) + '; Output: ' + str(output))
                    hit_list.append(output)
            #store rule result
            rule_result = bool(conditions == sum(hit_list))
            print_verbose(verbose,'\t\tRULE RESULT: ' + str(rule_result) + '; Conditions available: ' + str(conditions) + '; Conditions met: ' + str(sum(hit_list)))

            if run_all == True:
                if rule_result == True:
                    output_dict['audit_trail']['rules'].append(rule_name)
                    output_dict['audit_trail']['match'].append(rule_result)
                    output_dict['audit_trail']['score'].append(rule_score)
                else: 
                    output_dict['audit_trail']['rules'].append(rule_name)
                    output_dict['audit_trail']['match'].append(rule_result)
                    output_dict['audit_trail']['score'].append(rule_score)
            else: 
                if rule_result == True:
                    output_dict['audit_trail']['rules'].append(rule_name)
                    output_dict['audit_trail']['match'].append(rule_result)
                    output_dict['audit_trail']['score'].append(rule_score)
                    break
        if sum(output_dict['audit_trail']['match']) > 0:
            output_dict['decision_table_result'] = True
            output_dict['decision_table_score'] = sum([match*score for match,score in zip(output_dict['audit_trail']['match'],output_dict['audit_trail']['score'])])
            output_dict['decision_table_hits'] = sum(output_dict['audit_trail']['match'])
        else:
            output_dict['decision_table_result'] = False
            output_dict['decision_table_score'] = sum([match*score for match,score in zip(output_dict['audit_trail']['match'],output_dict['audit_trail']['score'])])
            output_dict['decision_table_hits'] = sum(output_dict['audit_trail']['match'])
        return output_dict
    except Exception as e:
        print('ERROR: An error occured while applying decision table to data')
        print(e)          
        
###############################################################################
#function to save objects
###############################################################################
def save_object(obj, filename):
    """
    DESCRIPTION:
    This function is to save objects to a pickle file.
    
    PARAMETERS:
    obj (object; required) - An object to save.
    
    filename (str; required) - The filename the object should be saved as.
    
    OUTPUT/RESULT:
    The result of this function will be an object saved to a file.
    """
    with open(filename, 'wb') as output:  # Overwrites any existing file.
        pickle.dump(obj, output, -1)

###############################################################################
#function to import object
###############################################################################
        
def import_object(filename):
    """
    DESCRIPTION:
    This function is to import objects from a pickle file.
    
    PARAMETERS:
    filename (str; required) - The filename the object should be imported from.
    
    OUTPUT/RESULT:
    The result of this function will be an object.
    """
    with open(filename, 'rb') as f:
        obj = pickle.load(f)
    return obj

###############################################################################
#function to create rule engine
###############################################################################
    
def create_rule_engine(directory, name=None, description=None, dataframe=False, return_object=True):
    """
    DESCRIPTION:
    This function is to create a rule engine object and necessary directories 
    and files for its setup.
    
    PARAMETERS:
    directory (str; required) - The directory to create the rule engine object
    and all accompanying directories and files.
        
    name (str; optional; default:Nonetype) - The name of the rule engine.
    
    description (str; optional; default:Nonetype) - A description for the rule
    engine.
    
    dataframe (bool; required; default:False) - Whether or not the rule engine
    should expect a pandas dataframe object as its input.
    
    return_object (required; default:True) - A parameter for whether or not to
    return a rule engine object upon creation.
    
    OUTPUT/RESULT:
    The result of this function is the creation of a rule engine object and all
    necessary directories and files for its setup.
    """
    ###########################################################################
    #create directories
    ###########################################################################

    try:
        print('Create directories')
        os.mkdir(directory + '/decision_tables')
        print(directory + '/decision_tables directory created')
        os.mkdir(directory + '/collections')
        print(directory + '/collections directory created')
    except Exception as e:
            print('ERROR: An error occured creating directories for rule engine')
            print(e)
    
    ###########################################################################
    #create rule engine object
    ###########################################################################  
    
    try:
        print('Create rule engine object')
        obj = rule_engine(main_directory = directory,
                          name = name,
                          description = description,
                          dataframe = dataframe)
        save_object(obj, directory + '/rule_engine_object.pkl')
        print('Rule engine created, next steps...')
        print('1 - Assign attributes and expressions to data structure')
        print('2 - Create decision table/s')
        print('3 - Create collection/s and update configuration table/s')
        print('4 - Test to ensure it works')
        print('Optional - Create any custom functions to be utilized within rule engine components')

    except Exception as e:
            print('ERROR: An error occured creating rule engine object')
            print(e)
            
    ###########################################################################
    #return rule engine object
    ###########################################################################   
    
    if return_object == True:
        print('Return rule engine object')
        return obj
    else:
        del obj


###############################################################################
#function to print verbose
###############################################################################

def print_verbose(verbose, string):
    """
    DESCRIPTION:
    This function is to use the verbose settings within functions to print 
    steps as the rule engine and its components are executing.
    
    PARAMETERS:
    verbose (bool; required) - A parameter for whether or not to print the
    execution details of the component. This parameter is inherited from the
    object or function.
        
    string (str; required) - The string to print
    
    OUTPUT/RESULT:
    The result of this function is the printing of the execution steps of the
    rule engine component
    """
    if verbose == True:
        print(string)
            