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

>   * `-name` can be replaced with `-sname` for local run
>   * `ip` is the to be replaced with the ip address over the corresponding file is been run on
>   * Need to update `InputFiles/servers.txt` with the list of all the servers node available (replace `{i}` with the number of the server)

## Runtime video
You can find the video [HERE](https://www.youtube.com/watch?v=H0qoDcPcLVk)

## Features

[x] Master & Slaves monitoring each other `erlang::monitor_node(Node, true).`  
[]  Distribution of the files using tfp
[x] Parallelization  
[x] Digraph & [GraphViz](https://github.com/glejeune/erlang-graphviz)  
[x] Use of [gen_server module](https://erlang.org/doc/man/gen_server.html)

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
