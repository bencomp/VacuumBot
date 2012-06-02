#!/usr/bin/env python

"""VacuumBot, based on a script by another Ben
  https://github.com/internetarchive/openlibrary/blob/master/scripts/2010/04/import_goodreads_ids.py
  
  Provides tools to remove given keys from OL records, to de-duplicate values
  from a given key and to move values from one key to another. 
  Other purposes may be built in later.
  
  This is a work in progress.
  """

from time import localtime, sleep, strftime
from olapi import OpenLibrary



def print_log(msg):
  timestamp = strftime("%Y%m%d_%H:%M:%S", localtime())
  print("[" + timestamp + "] " + msg)

def set_identifier(book, id_name, id_value):
  # OL handles the standard identifiers in a different way.
  if id_name in ["isbn_10", "isbn_13", "oclc_numbers", "lccn"]:
    ids = book.setdefault(id_name, [])
    if id_value not in ids:
      ids.append(id_value)
  else:
    ids = book.setdefault("identifiers", {})
    ids[id_name] = [id_value]

def set_goodreads_id(olid, goodreads_id):
  book = ol.get(olid)
  set_identifier(book, "goodreads", goodreads_id)
  ol.save(book['key'], book, "Added goodreads ID.")

def map_id(olid, isbn, goodreads_id):
  book = ol.get(olid)
  if book.has_key('identifiers'):
    if book['identifiers'].has_key('goodreads'):
      if goodreads_id in book['identifiers']['goodreads']:
        return
  print_log("Adding Goodreads ID \"" + goodreads_id + "\" to Openlibrary ID \"" + olid + "\"")
  set_goodreads_id(olid, goodreads_id)

class VacuumBot:
  """VacuumBot can help clean up Open Library, just tell it what to do!
  
  The VacuumBot has (its) methods to do specific cleanup tasks.
  It needs the credentials of a bot account on Open Library and some instructions.
  Naturally, it shows no mercy.
  """
  
  def __init__(self, username, password):
    self.ol = OpenLibrary()
    self.ol.login(username, password)
  
  def remove_classification_value(self, obj, type, value):
    """Removes a value from the list of <type> classifications.
    
    For example, can be used to remove the "B" value from 
    Dewey Decimal classifications.
    If the classifications list is empty afterwards, it is removed.
    If the classifications object in the record is empty (because
    removing the deleted list was the only one in it), it is removed 
    as well.
    """
    special = ["lc_classifications", "dewey_decimal_class"]
    if type in special and type in obj.keys():
      while value in obj[type]:
        obj[type].remove(value)
      if len(obj[type]) == 0:
        del obj[type]
    elif "classifications" in obj.keys() and type in obj["classifications"].keys():
      while value in obj["classifications"][type]:
        obj["classifications"][type].remove(value)
      if len(obj["classifications"][type]) == 0:
        del obj["classifications"][type]
        if len(obj["classifications"]) == 0:
          del obj["classifications"]
   
  def deduplicate_list(self, li):
    """Sorts a list and removes duplicate values in place."""
    
    a = len(li)
    c = 0
    li.sort()
    while c < a-1:
      if li[c] == li[c+1]:
        li.pop(c+1)
        a = a-1
      else:
        c = c+1
    
  def dedup(self, obj):
    """Removes duplicate values from an object.
    
    Calls deduplicate_list for lists.
    Calls itself on compound objects.
    Does nothing with strings or other types.
    """
    if isinstance(obj, str):
      return
    elif isinstance(obj, dict):
      for k in obj:
        dedup(obj[k])
    elif isinstance(obj, list):
      deduplicate_list(obj) 
    else:
      return

  def remove_key(self, olid, key):
    """Removes a key from a record
    
    Use with caution :)
    """
    object = ol.get(olid)
    if key in object:
      del object[key]
      ol.save(object['key'], object, "Sucked up \"" + key + "\".")
    

  def deduplicate_values(self, olid, key):
    """Removes duplicate values
    
    Reads the values of a key and removes duplicate values,
    leaving 1.
    """
    object = ol.get(olid)
    if key in object:
      dedup(object[key])

  def remove_classification(self, obj, classification):
    if "classifications" in obj:
      if classification in obj["classifications"]:
        del obj["classifications"][classification]

  def clean_lccn_permalink(self, olid):
    """Removes lccn_permalink from classifications
    
    Removes permalink from classifications and adds the LCCN to
    the identifiers, if is isn't there already.
    """
    object = ol.get(olid)
    if "classifications" in object.keys():
      if "lccn_permalink" in object["classifications"].keys():
        lccnst = []
        for a in object["classifications"]["lccn_permalink"]:
          b = a.rsplit("/",1)
          lccnst.append(b[len(b)])
        if "lccn" not in object.keys():
          object["lccn"] = []
        for l in lccnst:
          if l not in object["lccn"]:
            object["lccn"].append(l)
        remove_classification(object, "lccn_permalink")
    

  def vacuum(self, filename):
    """Main execution
    
    Vacuums the Open Library based on commands found in the file.
    
    Commands understood by VacuumBot are:
    * remove_key
    
    Command files are structured as follows: [todo]
    
    """
    n = 0
    for line in open(filename):
      olid, isbn, goodreads_id = line.strip().split()
      n = n+1
      if (n % 100000) == 0:
        print_log("(just read line " + str(n) + " from the command file)")
      is_good = False
      while (not is_good):
        try:
          map_id(olid, isbn, goodreads_id)
          is_good = True
        except:
          print_log("Exception for Goodreads ID \"" + goodreads_id + "\", message: \"" + str(sys.exc_info()[1]) + "\"")
          sleep(10)

#if __name__ == "__main__":
#  import sys
#  vacuum(sys.argv[1])
