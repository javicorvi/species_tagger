import sys
import argparse
import ConfigParser
import re
import codecs
import os
from subprocess import call

import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

parser=argparse.ArgumentParser()
parser.add_argument('-p', help='Path Parameters')
args=parser.parse_args()
parameters={}
if __name__ == '__main__':
    import species_tagger
    parameters = species_tagger.ReadParameters(args)     
    species_tagger.Main(parameters)

def Main(parameters):
    inputDirectory=parameters['inputDirectory']
    outputDirectory= parameters['outputDirectory']
    index_id=int(parameters['index_id'])
    index_text_to_tag= int(parameters['index_text_to_tag'])
    tagging(inputDirectory, outputDirectory, index_id, index_text_to_tag)
    
    
    
def ReadParameters(args):
    if(args.p!=None):
        Config = ConfigParser.ConfigParser()
        Config.read(args.p)
        parameters['inputDirectory']=Config.get('MAIN', 'inputDirectory')
        parameters['outputDirectory']=Config.get('MAIN', 'outputDirectory')
        parameters['index_id']=Config.get('MAIN', 'index_id')
        parameters['index_text_to_tag']=Config.get('MAIN', 'index_text_to_tag')
    else:
        logging.error("Please send the correct parameters config.properties --help ")
        sys.exit(1)
    return parameters   

def tagging(input_file, output_file, index_id, index_text_to_tag):
    if not os.path.exists(output_file):
        os.makedirs(output_file)
    ids_list=[]
    if(os.path.isfile(output_file+"/list_files_processed.dat")):
        with open(output_file+"/list_files_processed.dat",'r') as ids:
            for line in ids:
                ids_list.append(line.replace("\n",""))
        ids.close()
    if os.path.exists(input_file):
        onlyfiles_toprocess = [os.path.join(input_file, f) for f in os.listdir(input_file) if (os.path.isfile(os.path.join(input_file, f)) & f.endswith('.xml.txt') & (os.path.basename(f) not in ids_list))]
    
    with open(output_file+"/list_files_processed.dat",'a') as list_files:    
        for file in onlyfiles_toprocess:    
            output_file_result = output_file + os.path.basename(file)
            process(file, output_file_result, index_id, index_text_to_tag)
            list_files.write(os.path.basename(file)+"\n")
            list_files.flush()
    list_files.close() 

def process(input_file, output_file_result, index_id, index_text_to_tag):
    logging.info("Tagging  intup file  : " + input_file + ".  output file : "  + output_file_result)
    if not os.path.exists(output_file_result):
        os.makedirs(output_file_result)
    total_articles_errors = 0
    with codecs.open(input_file,'r',encoding='utf8') as file:
        for line in file:
            try:
                data = re.split(r'\t+', line) 
                if(len(data)==5):
                    id = data[index_id]
                    text_to_tag = data[index_text_to_tag]
                    with codecs.open(output_file_result+"/"+ id + ".txt",'w',encoding='utf8') as f:
                        f.write(text_to_tag)
                        f.flush()
                else:
                    logging.error("The article with line:  " + line)
                    logging.error("Belongs to : " + input_file + " and does not have four columns")
                    total_articles_errors = total_articles_errors + 1
            except Exception as inst:
                logging.error("The article with id : " + id + " could not be processed. Cause:  " +  str(inst))
                logging.error("Belongs to : " + input_file )
                logging.debug( "Full Line :  " + line)
                logging.error("The cause probably: contained an invalid character ")
                total_articles_errors = total_articles_errors + 1
        file.close()           
                
    call_species_tagger(output_file_result, output_file_result+"_tagged.txt")
    logging.info("Tagging  Finish For " + input_file + ".  output file : "  + output_file_result + ", articles with error : " + str(total_articles_errors))    
        


def call_species_tagger(input_folder, output_file):
    with open("species_tagger_linnaeus.log", "w") as result_file:
        resp = call(["java", "-Xmx4g", "-jar", "lib/linnaeus-2.0.jar", "--textDir", input_folder, 
               "--out", output_file], stdout=result_file)
        if(resp==1):
            logging.error("Linnaeus error, Tagging input folder  : " + input_folder + ".  output file : "  + output_file)
    
