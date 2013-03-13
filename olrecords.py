

class OLRecord:
  """Representation of a record in Open Library.
  """
  def __init__(self, olid):
    self.r = {}

  def cleanUp(self):
    pass 


class OLEdition(OLRecord):
  """Representation of an Edition in Open Library.
  """
  
  def __init__(self, olid):
    OLRecord.__init__(olid)
    
  def get_title(self):
    return self.r["title"] or None
    