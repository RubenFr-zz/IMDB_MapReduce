# Final Project - Map Reduce in Erlang over IMDb

> You can find the data base [HERE](https://datasets.imdbws.com)

## Run

> __Note:__ Every file are already compiled

### Server (Worker)
```bash
erl -name server{i}@ip -setcookie x -run server start_link
```

### Master
```bash
erl -name master@ip -setcookie x -run master start_link
```

### Client
```bash
erl -name client@ip -setcookie x
client:start().
```

>   * `-name` can be `-sname` for local connection run
>   * `ip` is the to be replaced with the ip address over the corresponding file is been run on
>   * Need to update `InputFiles/servers.txt` with the list of all the servers node available (replace `{i}` with the number of the server)


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

> __8.163.533 films/tv shows/shorts__

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

## Runtime video
You can find the video [HERE](https://youtu.be/gCopIflEc6I)

