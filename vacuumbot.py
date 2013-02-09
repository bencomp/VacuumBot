#!/usr/bin/env python

"""VacuumBot, based on a script by another Ben
  https://github.com/internetarchive/openlibrary/blob/master/scripts/2010/04/import_goodreads_ids.py
  
  Provides tools to remove given keys from OL records, to de-duplicate values
  from a given key and to move values from one key to another. 
  Other purposes may be built in later.
  
  This is a work in progress.
  """

from time import localtime, sleep, strftime
from openlibrary.api import OpenLibrary, OLError, marshal, unmarshal, Text, Reference
import codecs, re, simplejson, sys

import nomenklatura
import itertools
"""
class NKCache(object):
"""
"""Interface to the Nomenklatura reconciliation database with local caching.
  
  Code for this class was adapted from 
  http://schoolofdata.org/handbook/recipes/reconciling-data-with-nomenklatura/
"""
"""def __init__(self,dataset,key):
      self.ds=nomenklatura.Dataset(dataset,api_key=key)
      self.fetch()

  def fetch(self):
      links=itertools.ifilter(lambda x: x.value: self.ds.links())
      self.cache=dict(((l.key,l.value["value"]) for l in links))

  def lookup(self,key):
      if key in self.cache.keys():
         return self.cache[key]
      else:
          try:
              value=self.ds.lookup(key)
              return value.value
          except self.ds.NoMatch:
              return key
"""

def print_log(msg):
  timestamp = strftime("%Y-%m-%d_%H:%M:%S", localtime())
  print(unicode("[" + timestamp + "] " + msg).encode("utf-8"))

class OLBuffer:
  """Tools to buffer Open Library API interactions.
  
  """
  def __init__(self):
    pass
  
class VacuumBot:
  """VacuumBot can help clean up Open Library, just tell it what to do!
  
  The VacuumBot has (its) methods to do specific cleanup tasks.
  It needs the credentials of a bot account on Open Library and some instructions.
  Naturally, it shows no mercy.
  """
  
  def __init__(self, username, password):
    """Takes a username and password of a bot account to establish a connection to OL.
    
    """
    self.ol = OpenLibrary()
    self.ol.login(username, password)
    self.pagreg = re.compile(r"[^\s]\s+[:;]$")
    self.emptypagreg = re.compile(r"[,.:;]+$")
    self.formatdict = simplejson.load(codecs.open("formatdict.json", "rb", "utf-8"))
    self.enc2 = codecs.getencoder("ascii")
    self.savebuffer = {}
    self.badrecords = []
    self.aucache = {}
    self.wocache = {}
    #self.formatcache = NKCache("ol_books_formats", api_key = "cfdeaeda-4a22-4ae7-a2bf-1634da98fa1b")
    self.logfile = codecs.EncodedFile(open("vacuumbot-log.tsv", "ab"), "unicode_internal", "utf-8", "replace")
  
  def enc(self, str):
    return self.enc2(str, "backslashreplace")[0]
  
  def flog(self, key, operation, message):
    """Log to file 'vacuumbot-log.tsv'. Lines are time, key, operation and message, tab-separated.
    """
    self.logfile.write(unicode(strftime("%Y-%m-%d_%H:%M:%S", localtime()) + "\t" + key + "\t" + operation + "\t" + message + "\n"))
  
  def save_error(self, key, message):
    errorfile = codecs.EncodedFile(open("vacuumbot-errors.txt", "ab"), "unicode_internal", "utf-8", "replace")
    errorfile.write(unicode("[" + strftime("%Y-%m-%d_%H:%M:%S", localtime()) + "] Could not save record for: " + key + ", error was: " + message + "\n"))
    errorfile.close()
  
  def query(self, query):
    return self.ol.query(query)
  
  def ol_save(self, key, record, message):
    try:
      self.ol.save(key, record, self.enc(message))
      self.flog(key, "direct save", message)
      print_log("Saved "+key+": "+message)
    except OLError as e:
      self.save_error(key, str(e))
      print_log("Save failed: "+str(e))
  
  def ol_save2(self, key, record, message):
    if message != None:
      record = marshal(record)
      if message in self.savebuffer.keys():
        self.savebuffer[message][key] = record
        if len(self.savebuffer[message]) >= 100:
          self.flush(message)
      else:
        self.savebuffer[message] = {}
        self.savebuffer[message][key] = record
      self.flog(key, "buffer save", message)
    else:
      raise Exception("Message for saving is missing!")
  
  def flush(self, buffer_name):
    try:
      if len(self.savebuffer[buffer_name]) > 0:
        self.ol.save_many(self.savebuffer[buffer_name].values(), self.enc(buffer_name))
        for key in self.savebuffer[buffer_name].keys():
          self.flog(key, "buffer flush", buffer_name)
        print_log("Flushed buffer ("+str(len(self.savebuffer[buffer_name]))+" records): "+buffer_name)
        self.savebuffer[buffer_name] = {}
        sleep(1)
    except OLError as e:
      # Try to remove rejected record from buffer
      err_mess = simplejson.loads(re.sub(r'^[^{]*', "", str(e)))
      if err_mess["error"] == "bad_data":
        k = err_mess["at"]["key"]
        del self.savebuffer[buffer_name][k]
        self.save_error(k, "Multisave failed: "+str(e)+"; removed record from buffer")
      else:
        k = self.savebuffer[buffer_name].keys()[0]
        self.save_error(k, "Multisave failed: "+str(e))
  
  def flush_all(self):
    for m in self.savebuffer.keys():
      self.flush(m)
  
  def ol_get(self, key, v=None):
    """Gets a record from OL and catches OLErrors.
    
    Make sure you check for None when you process this function's result.
    """
    try:
      return self.ol.get(key, v)
    except OLError as e:
      self.save_error(key, str(e))
      print_log("Get failed: "+str(e))
  
  def clean_author_dates(self):
    for year in range(1900, 2013):
      authors = self.query({"type": "/type/author", "death_date": str(year)+".", "limit": False})
      print_log("Getting authors with death date '" + str(year) + "'...")
      for author in authors:
        obj = self.ol_get(author)
        self.clean_author(obj)
        sleep(2)
          
      done = codecs.EncodedFile(open("cleanauthors-done.txt", "ab"), "unicode_internal", "utf-8", "replace")
      done.write("Death date '" + str(year) + ".' updated to '" + str(year) + "'\n")
      done.close()
    
  def clean_author_dates2(self):
    for year in range(0,1000):
      # Get keys of all authors with death date <x>
      authors = self.query({"type": "/type/author", "death_date": str(year)+".", "limit": False})
      print_log("Getting authors with death date '" + str(year) + "'...")
      list = []
      for author in authors:
        # Feed authors to buffer list
        list.append(author)
        if len(list) > 99:
          # Process these few authors before continuing feeding
          # Get records
          print_log("Getting full records")
          
          records = self.ol.get_many(list)
          for obj in records.itervalues():
            self.clean_author2(obj)
          list = []
          
      if len(list) > 0:
        records = self.ol.get_many(list)
        for obj in records.itervalues():
          self.clean_author2(obj)
      self.flush_all()
      done = codecs.EncodedFile(open("cleanauthors-done.txt", "ab"), "unicode_internal", "utf-8", "replace")
      done.write(unicode("Death date '" + str(year) + ".' updated to '" + str(year) + "'\n"))
      done.close()
      sleep(0.5)
  
  def clean_author(self, obj):
    """Clean author records. For example removes the period after the death date.
    
    Saves the updated record directly, instead of putting it in the save buffer.
    """
    # Remove period from death date
    comment = []
    result1 = self.clean_death_date(obj)
    if result1 != None:
      comment.append("Removed period from death date")
      
    if len(comment) > 0:
      self.ol_save(obj["key"], result1, "; ".join(comment))
  
  def clean_author2(self, obj):
    """Clean author records. For example removes the period after the death date.
    
    Saves the updated record in the save buffer.
    """
    # Remove period from death date
    comment = []
    result1 = self.clean_death_date(obj)
    if result1 != None:
      comment.append("Removed period from death date")
      
    if len(comment) > 0:
      self.ol_save2(obj["key"], result1, "; ".join(comment))
  
  def clean_death_date(self, obj):
    changed = False
    if "death_date" in obj.keys():
      if re.match(r"^\d{1,4}\.", obj["death_date"]):
        obj["death_date"] = obj["death_date"].rstrip(" .")
        changed = True
      elif obj["death_date"] == "":
        del obj["death_date"]
        changed = True
    
    if changed:
      return obj
    else:
      return None
  
  def update_author_in_edition(self):
    aucache = {}
    for line in open("errors3.txt", "rb"):
      error = simplejson.loads(line)
      if error["error"] == "bad_data":
        olid = error["at"]["key"]
        try:
          obj = self.ol_get(olid)
          print "Edition found and downloaded"
          if "works" in obj.keys() and len(obj["works"]) > 0:
            # Get work, see if it has at least one author
            work = self.ol_get(obj["works"][0])
            print "Work found and downloaded"
            if "authors" in work.keys() and len(work["authors"]) > 0:
              del obj["authors"]
              self.ol_save(olid, obj, "Removed author from Edition (author found in Work)")
          else:
            # Get author and find new author key
            if "authors" in obj.keys() and len(obj["authors"]) > 0:
              newau = []
              for auID in obj["authors"]:
                if auID in aucache.keys():
                  # already looked up and stored in cache
                  newau.append(aucache[auID])
                  print "new ID found in cache"
                else:
                  # lookup author's new ID
                  newID = self.find_new_author(auID)
                  aucache[auID] = newID
                  newau.append(newID)
              obj["authors"] = newau
              self.ol_save(olid, obj, "Updated author in Edition (author was merged but not updated here)")
        except:
          print "Error trying to fix", olid
  
  
  def find_new_author(self, olid):
    obj = self.ol_get(olid)
    print "downloaded", olid
    if obj["type"] == "/type/author":
      return obj["key"]
    elif obj["type"] == "/type/redirect":
      return self.find_new_author(obj["location"])
    elif obj["type"] == "/type/delete":
      # author was deleted, undelete
      obj["type"] = "/type/author"
      self.ol_save(obj["key"], obj, "Undeleted record, because other records still referred to it")
      return obj["key"]

  def clean_physical_object(self, obj):
    """Cleans up physical aspects of the given Edition object, such as format and pagination. Returns the cleaned obj.
    
    Physical format: calls self.clean_format(obj).
    Pagination: calls self.clean_pagination(obj).
    """
    obj1 = self.clean_format(obj)
    if obj1 != None:
      obj2 = self.clean_pagination(obj1)
    else:
      obj2 = self.clean_pagination(obj)
    
    if obj2 != None:
      return obj2
    else:
      return obj
  
  def clean_format(self, obj):
    """Cleans up the obj's physical_format field, removing it from obj if necessary.
    
    Checks the current value of physical_format, looks up the replacement value and updates if new value is different from current value. If the format field is empty after this, it is removed.
    
    If nothing changed or there is no physical_format field to begin with, None is returned.
    """
    if "physical_format" in obj.keys():
      if obj["physical_format"] == "":
        # Remove empty field and return
        del obj["physical_format"]
        return obj
      else:
        # Check if there is a better format
        v = self._check_format(obj["physical_format"])
        if v != "":
          # Use new value
          obj["physical_format"] = v
          return obj
        elif v == "":
          # New value would leave empty field -> remove field
          del obj["physical_format"]
          return obj
        else:
          return None
        
    else:
      return None
  
  def _check_format(self, format):
    # if format matches punctuation regular expression, create normalized format string before lookup
    checkstr = re.sub(r"", "", format)
    # and look up replacement format in dictionary
    if checkstr in self.formatdict.keys() and self.formatdict[checkstr] != format:
      return self.formatdict[checkstr]
    else:
      # if there is no new value or new is same as original, don't update.
      return False
  
  def replace_formats(self, old, new):
    """Replaces the old value in physical format fields by the new value.
    
    This method tries to process all records with old as format value, which are potentially millions of records.
    """
    print_log("Getting records with format '"+old+"'...")
    olids = self.ol.query({"type":"/type/edition", "physical_format": old, "limit": False})
    for r in olids:
      print "Improving", r
      self.replace_format(r, old, new)
      sleep(4)
  
  def replace_formats_clean_pagination(self, old, new):
    """Replaces the old value in physical format fields by the new value.
    
    This method tries to process all records with old as format value, which are potentially millions of records.
    """
    print_log("Getting records with format '"+old+"'...")
    olids = self.ol.query({"type":"/type/edition", "physical_format": old, "limit": False})
    for olid in olids:
      gotit = False
      tries = 0
      while (not gotit and tries <= 5):
        obj = self.ol_get(olid)
        if obj:
          gotit = True
        else:
          sleep(10)
          tries = tries + 1
      
      if not gotit:
        raise Exception("timeout")
      
      comment = []
      print "Improving", olid
      # Step 1: replace format
      result1 = self.replace_format2(obj, old, new)
      if result1:
        comment.append("Updated format '"+old+"' to '"+new+"'")
      else:
        result1 = obj
      
      # Step 2: Use Step 1 output
      result2 = self.clean_pagination(result1)
      if result2:
        comment.append("cleaned up pagination")
      else:
        result2 = result1
      
      # Something changed if comment is not empty
      if len(comment) != 0:
        self.ol_save(obj["key"], result2, "; ".join(comment))
        print_log("; ".join(comment))
      else:
        print_log("Did nothing, really.")
      sleep(3)

  def replace_split_formats_clean_pagination(self, old, new, by, sub, ot):
    """Replaces the old value in physical format fields by the new value and puts the by statement in the correct place.
    
    This method tries to process 1000 records with old as format value, which are potentially millions of records.
    """
    print_log("Getting records with format '"+old+"'...")
    olids = self.ol.query({"type":"/type/edition", "physical_format": old, "limit": 1000})
    list = []
    for olid in olids:
      # Feed authors to buffer list
      list.append(olid)
      if len(list) > 99:
        # Process these few authors before continuing feeding
        # Get records
        print_log("Getting full records")
        records = self.ol.get_many(list)
        for obj in records.itervalues():
          obj = unmarshal(obj)
          self._replace_split_formats_clean_pagination(obj, old, new, by, sub, ot)
        list = []
        
    if len(list) > 0:
      records = self.ol.get_many(list)
      for obj in records.itervalues():
        obj = unmarshal(obj)
        self._replace_split_formats_clean_pagination(obj, old, new, by, sub, ot)
    self.flush_all()

    
  def replace_formats_clean_pagination2(self, old, new):
    """Replaces the old value in physical format fields by the new value.
    
    This method tries to process all records with old as format value, which are potentially millions of records.
    """
    print_log("Getting records with format '"+old+"'...")
    olids = self.ol.query({"type":"/type/edition", "physical_format": old, "limit": 1000})
    list = []
    for olid in olids:
      # Feed authors to buffer list
      list.append(olid)
      if len(list) > 99:
        # Process these few authors before continuing feeding
        # Get records
        print_log("Getting full records")
        records = self.ol.get_many(list)
        for obj in records.itervalues():
          obj = unmarshal(obj)
          self._replace_formats_clean_pagination(obj, old, new)
        list = []
        
    if len(list) > 0:
      records = self.ol.get_many(list)
      for obj in records.itervalues():
        obj = unmarshal(obj)
        self._replace_formats_clean_pagination(obj, old, new)
    self.flush_all()
    
  
  def _replace_formats_clean_pagination(self, obj, old, new):
    comment = []
    # Step 1: replace format
    result1 = self.replace_format2(obj, old, new)
    if result1[1]:
      comment.append(result1[1])

    # Step 2: Use Step 1 output
    result2 = self.clean_pagination(result1[0])
    if result2[1]:
      comment.append(result2[1])
    
    # Step 3: use Step 2 output
    result3 = self._update_author_in_edition(result2[0])
    if result3[1]:
      comment.append(result3[1])

    # Something changed if comment is not empty
    if len(comment) != 0:
      self.ol_save2(obj["key"], result3[0], "; ".join(comment))

  def _replace_split_formats_clean_pagination(self, obj, old, new, by):
    comment = []
    # Step 1: replace format
    result1 = self.replace_format2(obj, old, new)
    if result1[1]:
      comment.append(result1[1])
    
    # Step 1a: add the By statement
    result1a = self.add_by(result1[0], by)
    if result1a[1]:
      comment.append(result1a[1])
    
    # Step 2: Use Step 1a output
    result2 = self.clean_pagination(result1a[0])
    if result2[1]:
      comment.append(result2[1])
    
    # Step 3: use Step 2 output
    result3 = self._update_author_in_edition(result2[0])
    if result3[1]:
      comment.append(result3[1])

    # Something changed if comment is not empty
    if len(comment) != 0:
      self.ol_save2(obj["key"], result3[0], "; ".join(comment))
      
  def _replace_split_formats_clean_pagination(self, obj, old, new, by, sub, ot):
    comment = []
    # Step 1: replace format
    result1 = self.replace_format2(obj, old, new)
    if result1[1]:
      comment.append(result1[1])
    
    # Step 1a: add the By statement
    result1a = self.add_by(result1[0], by)
    if result1a[1]:
      comment.append(result1a[1])
      
    # Step 1b: add the subtitle
    result1b = self.add_subtitle(result1a[0], sub)
    if result1b[1]:
      comment.append(result1b[1])
      
    # Step 1c: add the other_titles
    result1c = self.add_subtitle(result1b[0], ot)
    if result1c[1]:
      comment.append(result1c[1])
    
    # Step 2: Use Step 1b output
    result2 = self.clean_pagination(result1b[0])
    if result2[1]:
      comment.append(result2[1])
    
    # Step 3: use Step 2 output
    result3 = self._update_author_in_edition(result2[0])
    if result3[1]:
      comment.append(result3[1])

    # Something changed if comment is not empty
    if len(comment) != 0:
      self.ol_save2(obj["key"], result3[0], "; ".join(comment))

  def add_by(self, obj, by):
    if by != "":
      if "by_statement" in obj.keys():
        if len(obj["by_statement"]) == 0:
          obj["by_statement"] = by
          return (obj, "updated By statement")
        elif "notes" in obj.keys(): # and "value" in obj["notes"].keys()
          print self.enc(obj["notes"])
          if isinstance(obj["notes"], Text):
            obj["notes"] = obj["notes"] + "\n\nBy statement found in format: " + by + "\n"
            return (obj, "added By statement to notes, because By statement field was not empty")
          elif isinstance(obj["notes"], dict):
            d = unmarshal(obj["notes"])
            obj["notes"] = d + "\n\nBy statement found in format: " + by + "\n"
            return (obj, "added By statement to notes, because By statement field was not empty")
          else:
            obj["notes"] = Text("By statement found in format: " + by + "\n")
            return (obj, "put By statement in notes")
        else:
          obj["notes"] = Text("By statement found in format: " + by + "\n")
          return (obj, "put By statement in notes")
      else:
        obj["by_statement"] = by
        return (obj, "added By statement")
    else:
      return (obj, None)
      
  def add_subtitle(self, obj, sub):
    if sub != "":
      if "subtitle" in obj.keys():
        if len(obj["subtitle"]) == 0:
          obj["subtitle"] = sub
          return (obj, "updated subtitle")
        elif "notes" in obj.keys():
          print self.enc(obj["notes"])
          obj["notes"] = obj["notes"] + "\n\nSubtitle found in format: " + sub + "\n"
          return (obj, "added subtitle to notes, because subtitle field was not empty")
        else:
          obj["notes"] = sub
          return (obj, "put subtitle in notes")
      else:
        obj["subtitle"] = sub
        return (obj, "added subtitle")
    else:
      return (obj, None)

  def add_other_title(self, obj, ot):
    if ot != "":
      if "other_titles" in obj.keys():
        if isinstance(obj["other_titles"], list):
          obj["other_titles"].append(ot)
          return (obj, "added title to other_titles")
        elif isinstance(obj["other_titles"], string) or isinstance(obj["other_titles"], unicode):
          obj["other_titles"] = [obj["other_titles"], ot]
          return (obj, "changed other_titles from string to list of strings")
        else:
          raise Exception("other_titles is not a list or string")
      else:
        obj["other_titles"] = [ot]
        return (obj, "created other_titles")
    else:
      return (obj, None)
      
  def _update_author_in_edition(self, obj):
    #try:
      workhasauthors = False
      hasauthors = "authors" in obj.keys() and len(obj["authors"]) > 0
      
      if not hasauthors:
        return (obj, None)
      elif "works" in obj.keys() and len(obj["works"]) > 0:
        #print obj["works"][0]
        wID = obj["works"][0]
        if wID in self.wocache.keys():
          print "Work ID found in cache", wID
          workhasauthors = self.wocache[wID]
        else:
          # Get work, see if it has at least one author
          work = self.ol_get(wID)
          print "Work found and downloaded", wID
          self.wocache[wID] = "authors" in work.keys() and len(work["authors"]) > 0
          workhasauthors = self.wocache[wID]
        
        if workhasauthors and "authors" in obj.keys():
          del obj["authors"]
          return (obj, "Removed author from Edition (author found in Work)")
        else:
          # Edition has authors, Work has no authors. Work should be updated and then authors removed from edition.
          # for now, update authors in edition (that causes error). Leave rest to WorkBot.
          return self._replace_authors(obj)
          
      else:
        return self._replace_authors(obj)
    #except Exception as e:
     # print "Error trying to fix", obj["key"]
      #print sys.exc_info()
  
  def _replace_authors(self, obj):
    newau = []
    for auID in obj["authors"]:
      if auID in self.aucache.keys():
        # already looked up and stored in cache
        newau.append(Reference(self.aucache[auID]))
        print "new ID found in cache"
      else:
        # lookup author's new ID
        newID = self.find_new_author(auID)
        self.aucache[auID] = newID
        newau.append(Reference(newID))
      
    print obj["authors"]
    print newau
    if obj["authors"] != newau:
      obj["authors"] = newau
      comment = "replaced author(s) in Edition (reference was outdated)"
      return (obj, comment)
    else:
      return (obj, None)
  
  def replace_format(self, olid, old, new):
    """Replaces a value from the physical format field.
    
    """
    try:
      obj = self.ol.get(olid)
    except OLError as e:
      self.save_error(olid, str(e))
      return
    
    if "physical_format" in obj.keys() and obj["physical_format"] == old:
        obj["physical_format"] = new
        print_log("updating format for "+olid)
        self.ol_save(obj["key"], obj, "Updated format '"+old+"' to '"+new+"'")
  
  def replace_format2(self, obj, old, new):
    """Replaces a value from the physical format field.
    
    Returns a tuple (obj, comment). comment is None if nothing changed. obj is updated or unchanged input obj.
    """
    
    if "physical_format" in obj.keys() and obj["physical_format"] == old:
      obj["physical_format"] = new
      print_log("updating format for "+obj["key"])
      return (obj, "Updated format '"+old+"' to '"+new+"'")
    elif "physical_format" in obj.keys() and obj["physical_format"] == "":
      del obj["physical_format"]
      return (obj, "Deleted empty physical format field")
    else:
      return (obj, None)
  
  def clean_pagination(self, obj):
    """Removes spaces, semicolons and colons from the end of the pagination field of obj.
    
    If the pagination field is empty or only contains a combination of ',', '.', ';' and ':', the field is removed.
    
    Returns a tuple of the possibly updated obj and a comment (which is None if nothing changed).
    """
    if "pagination" in obj.keys():
      # Strip commas, semicolons, colons, forward slashes and spaces from the right.
      new = obj["pagination"].rstrip(" ,;:/")
      if obj["pagination"] == new:
        # Pagination did not change.
        return (obj, None)
      elif obj["pagination"] == "" or new == "" or self.emptypagreg.match(obj["pagination"]):
        # field is empty, or only has ignorable characters, remove the field.
        del obj["pagination"]
        return (obj, "deleted empty pagination field")
      else:
        obj["pagination"] = new
        return (obj, "cleaned up pagination")
        
    else:
      # There is no pagination field; return None.
      return (obj, None)
    
  
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
      self.ol_save(object['key'], object, "Sucked up \"" + key + "\".")
    

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
