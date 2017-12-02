# PYBABAR

This repo is in the very early stages on implementing in Python some of the functionality provided by BABAR, a knowledge extraction system for Wikipedia written entirely in CLOS.

Paper: https://www.academia.edu/8743235/BABAR_Wikipedia_Knowledge_Extraction  
Slides: //www.slideshare.net/delaray/knowledge-extraction-presentation  
Video: https://vimeo.com/49349122  

The initial goal is to scrape and persist the entire Wikipedia graph structure and provide naviagtion, organization, querying and reasoning tools based on this graph structure.

More details coming soon...

# Examples 

### 1. Find the list of Wikipedia topics pointed to by the Elephant page...

In [43]: x= find_wiki_out_neighbors("Elephant")

In [44]: x.sort()

In [45]: len(x)
Out[45]: 588

In [46]: x
Out[46]: ['A_Greek%E2%80%93English_Lexicon', 'Abdominal_cavity',
 'Aberdare_National_Park', 'Achaemenid_Empire', 'Adaptive_radiation', 'African_bush_elephant',..., 'Zygolophodon']

### 2. Find the list of Wikipedia topics that point to the Elephant page...

In [47]: x= find_wiki_in_neighbors("Elephant")

In [48]: x.sort()

In [49]: len(x)
Out[49]: 119

In [50]: x
Out[50]: ['African_bush_elephant', 'African_forest_elephant', 'Amebelodontidae', 'Anancidae', 'Anancus', 'Animal_track', 'Archaeobelodon', 'Asian_elephant', 'Babar_the_Elephant', 'Barytheriidae', 'Borneo_elephant', 'War_elephant', 'Woolly_mammoth', 'Working_animal', 'Year_of_the_Elephant', 'Zoo']

### 3. Find the list of Wikipedia topics that are mutually linked to the Elephant page...


In [51]: x = find_strongly_related_topics("Elephant")

In [52]: x.sort()

In [53]: len(x)
Out[53]: 120

In [54]: x
Out[54]: ['African_bush_elephant', 'African_forest_elephant',..., 'Tusk', 'Vomeronasal_organ', 'War_elephant', 'Woolly_mammoth', 'Working_animal', 'Year_of_the_Elephant', 'Zoo']

4. Find the list of Wikipedia topics that are potential subtopics of "Elephant"