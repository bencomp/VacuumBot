VacuumBot
=========

Best used for cleaning records on [Open Library](http://openlibrary.org). If you are ready to do so, 
you need a bot account on <http://openlibrary.org>.  
The author's VacuumBot is <http://openlibrary.org/people/vacuumbot>.

VacuumBot is mostly a collection of methods for specific cleaning tasks written in Python, 
that call the Open Library Python client API. Hence you also need the 
[Open Library API client](https://github.com/internetarchive/openlibrary/blob/master/openlibrary/api.py).

Create an object from a separate script using the bot's credentials and 
call the methods that perform the desired actions.

Example usage:

    # Login
    vb = VacuumBot("user", "pass")
    
    # Replace the book format "pepperbek" by "Paperback" and trim punctuation
    #  in the pagination field.
    vb.replace_formats_clean_pagination2("pepperbek", "Paperback")
