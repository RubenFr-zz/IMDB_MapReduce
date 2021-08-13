# Final Project

## Tools that can be used
- Yaws
- Cowboys
- erlport (Python)
- Mnesia
- wxLab  

## Data Set

```text
tt0000009,Miss Jerry,Miss Jerry,1894,1894-10-09,Romance,45,USA,None,Alexander Black,Alexander Black,Alexander Black Photoplays,"Blanche Bayliss, William Courtenay, Chauncey Depew",The adventures of a female reporter in the 1890s.,5.9,154,,,,,1,2
```

|        Key        |                            Value                      | 
| :---------------: | :---------------------------------------------------: | 
|id| tt0000009 |
|title| Miss Jerry  |
original title | Miss Jerry  |
year | 1894  |
date published | 1894-10-09  |
genre | Romance  |
duration | 45  |
country | USA  |
language | None  |
director | Alexander Black  |
writer | Alexander Black  |
production company | Alexander Black Photoplays  |
actors | Blanche Bayliss, William Courtenay, Chauncey Depew|
description | The adventures of a female reporter in the 1890s.   |
avg vote | 5.9  |
votes | 154  |
reviews from users | 1  |
reviews from critics | 2|

## Map Reduce
The goal of the map reduce step will be to find the first link between all movies and actors.

### Mapper
#### input:

__id__: tt0000009  
__title__: Miss Jerry  
__original title__: Miss Jerry  
__year__: 1894  
__date published__: 1894-10-09  
__genre__: Romance  
__duration__: 45  
__country__: USA  
__language__: None  
__director__: Alexander Black  
__writer__: Alexander Black  
__production company__: Alexander Black Photoplays  
__actors__: Blanche Bayliss, William Courtenay, Chauncey Depew  
__description__: The adventures of a female reporter in the 1890s.   
__avg vote__: 5.9  
__votes__: 154  
__reviews from users__: 1  
__reviews from critics__: 2

#### output (sent to reducers):
|       Key         |                         Value                         | 
| :---------------: | :---------------------------------------------------: | 
|Miss Jerry         | Blanche Bayliss, William Courtenay, Chauncey Depew    | 
|Blanche Bayliss    | Chauncey Depew, William Courtenay                     |
|William Courtenay  | Blanche Bayliss, Chauncey Depew                       |
|Chauncey Depew     | Blanche Bayliss, William Courtenay                    |

### Reducer
#### input: 
K= Movie or Actor      V= List of actors

#### output:
Write to a file with the name: Key all the actors


## Input

#### basics.tsv
    - tconst (string) - alphanumeric unique identifier of the title  
    - titleType (string) – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
    - primaryTitle (string) – the more popular title / the title used by the filmmakers on promotional materials at the point of release  
    - originalTitle (string) - original title, in the original language  
    - isAdult (boolean) - 0: non-adult title; 1: adult title  
    - startYear (YYYY) – represents the release year of a title. In the case of TV Series, it is the series start year  
    - endYear (YYYY) – TV Series end year. ‘\N’ for all other title types  
    - runtimeMinutes – primary runtime of the title, in minutes  
#### principals.tsv  
    - tconst (string) - alphanumeric unique identifier of the title  
    - ordering (integer) – a number to uniquely identify rows for a given titleId  
    - nconst (string) - alphanumeric unique identifier of the name/person  
    - category (string) - the category of job that person was in  
    - job (string) - the specific job title if applicable, else '\N'  
    - characters (string) - the name of the character played if applicable, else '\N'  
#### names.tsv  
    - nconst (string) - alphanumeric unique identifier of the name/person  
    - primaryName (string)– name by which the person is most often credited  
    - birthYear – in YYYY format  
    - deathYear – in YYYY format if applicable, else '\N'  
    - primaryProfession (array of strings)– the top-3 professions of the person  
    - knownForTitles (array of tconsts) – titles the person is known for  

The objective is to merge the three file to fit our model:  
> Create one file per movie and crew member. 
> A movie file will contain the list of all the crew that played in it
> An crew file will contain the list of all the movies the actor played in

That way when we want to create the tree of a movie, we'll go over the file of all it's actors and that will be rank 1.
We'll do the same for every other level.

### Distribution

#### basics.tsv
This is the first file to proceed. It contains the name and id (`tconst`) of __8.163.533 films/tv shows/shorts__.  
The four computers will receive a quarter of the file.
The first step will be to remove unused information such as: originalTitle, isAdult, startYear, endYear, runtimeMinutes
and to keep: **tconst, titleType, primaryTitle, genres**

#### principals.tsv
In this file we have the list of the crew/cast for each title. We'll use this to update the record we got in the first step

#### names.tsv
Finaly this file contains information on each crew/cast member. We will use that file to update the nconst gotten in the previous step.
Because every computer will need these information, the main computer will deal with it and the slaves will send request for cast name.