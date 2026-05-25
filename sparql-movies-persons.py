import requests
import json
import os
from dotenv import load_dotenv
import time
import pymysql.cursors
import citizenphil as cp
from datetime import datetime
import csv
import pandas as pd
import re
import urllib.error
from SPARQLWrapper import SPARQLWrapper, SPARQLExceptions, JSON, POST

# Load .env file 
load_dotenv()

strwikidatauseragent = os.getenv("WIKIMEDIA_USER_AGENT")
print("strwikidatauseragent",strwikidatauseragent)

def f_sparqlpersonscrawl(strwikidataidquery,lngyearquery=0):
    global strwikidatauseragent
    global strsparqlpersoninstanceof
    
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?tmdbID ?birthDate ?deathDate ?instanceOf "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            # Accept either a single Wikidata id or a whitespace-separated list, so callers can batch
            arrwikidataidlist = [s.strip() for s in strwikidataidquery.split() if s.strip()]
            strwikidataidwd = " ".join([f"wd:{s}" for s in arrwikidataidlist])
            strsparqlquery += "VALUES ?item { " + strwikidataidwd + " } "
            strsparqlquery += "?item wdt:P31 ?instanceOf. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4985 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P569 ?birthDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
        else:
            arrpersoninstanceof = [s.strip() for s in strsparqlpersoninstanceof.split() if s.strip()]
            strpersoninstanceofwd = " ".join([f"wd:{s}" for s in arrpersoninstanceof])
            if strpersoninstanceofwd == "":
                strpersoninstanceofwd = "wd:Q5"
            strsparqlquery += "VALUES ?instanceOf { " + strpersoninstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?instanceOf; "
            strsparqlquery += "wdt:P345 ?imdbID; "
            strsparqlquery += "wdt:P569 ?birthDate. "
            strsparqlquery += "?item wdt:P31 ?instanceOf. "
            strsparqlquery += "OPTIONAL { ?item wdt:P4985 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
            if lngyearquery > 0:
                strsparqlquery += "FILTER(YEAR(?birthDate) = " + str(lngyearquery) + ") "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item "
        #strsparqlquery += "LIMIT " + str(lnglimit) + " "
        #strsparqlquery += "OFFSET " + str(lngoffset) + " "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        # Set the query and return format
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        # Execute the query and convert the results
        try:
            query_result = sparql.query()
            results = query_result.convert()
            #print(results)
            # Convert the results to a Pandas DataFrame
            df = pd.json_normalize(results['results']['bindings'])
            lngcount = 0
            if not df.empty:
                #df = df[['item.value', 'itemLabel.value', 'imdbID.value', 'tmdbID.value', 'birthDate.value']]
                for index, row in df.iterrows():
                    lngcount += 1
                    print(row)
                    stritem = row['item.value']
                    # Compute strwikidataid
                    strwikidataid = ""
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlerpersonscurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    # Compute strname
                    strname = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strname = row['itemLabel.value']
                                # reject any name that looks like a Wikidata ID
                                if re.match(r'^[QPL]\d+$', strname):
                                    strname = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                	strimdbid = strimdbid[:10]
                    # Compute lngtmdbid
                    lngtmdbid = 0
                    if 'tmdbID.value' in row:
                        if row['tmdbID.value']:
                            if not pd.isna(row['tmdbID.value']):
                                lngtmdbid = row['tmdbID.value']
                                #print(lngtmdbid)
                    # Compute birth date
                    strbirthdate = ""
                    strbirthdatesql = ""
                    if 'birthDate.value' in row:
                        if row['birthDate.value']:
                            if not pd.isna(row['birthDate.value']):
                                strbirthdate = row['birthDate.value']
                                #print(strbirthdate)
                                if strbirthdate != "":
                                    try:
                                        datbirthdate = datetime.strptime(strbirthdate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strbirthdatesql = datbirthdate.strftime("%Y-%m-%d %H:%M:%S")
                                        strbirthdatesql = datbirthdate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strbirthdate} cannot be converted to a date.")
                                        strbirthdatesql = ""
                    # Compute death date
                    strdeathdate = ""
                    strdeathdatesql = ""
                    if 'deathDate.value' in row:
                        print("'deathDate.value' in row")
                        if row['deathDate.value']:
                            if not pd.isna(row['deathDate.value']):
                                strdeathdate = row['deathDate.value']
                                print(strdeathdate)
                                if strdeathdate != "":
                                    try:
                                        datdeathdate = datetime.strptime(strdeathdate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strdeathdatesql = datdeathdate.strftime("%Y-%m-%d %H:%M:%S")
                                        strdeathdatesql = datdeathdate.strftime("%Y-%m-%d")
                                        print(strdeathdatesql)
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strdeathdate} cannot be converted to a date.")
                                        strdeathdatesql = ""
                    # Compute instance of
                    strinstanceof = ""
                    strinstanceofid = ""
                    if 'instanceOf.value' in row:
                        if row['instanceOf.value']:
                            if not pd.isna(row['instanceOf.value']):
                                strinstanceof = row['instanceOf.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    print(f"{strwikidataid} '{strimdbid}' ID {lngtmdbid} '{strname}' {strbirthdatesql}-{strdeathdatesql}")
                    arrpersoncouples = {}
                    arrpersoncouples["ID_WIKIDATA"] = strwikidataid
                    arrpersoncouples["ID_PERSON"] = lngtmdbid
                    arrpersoncouples["ID_IMDB"] = strimdbid
                    arrpersoncouples["NAME"] = strname
                    if strbirthdatesql != "":
                        arrpersoncouples["BIRTHDAY"] = strbirthdatesql
                    if strdeathdatesql != "":
                        arrpersoncouples["DEATHDAY"] = strdeathdatesql
                        print("arrpersoncouples[\"DEATHDAY\"] = ",strdeathdatesql)
                    arrpersoncouples["INSTANCE_OF"] = strinstanceofid
                    strsqltablename = "T_WC_WIKIDATA_PERSON_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrpersoncouples,strsqlupdatecondition,1)
            intencore = False
        except SPARQLExceptions.EndPointInternalError as e:
            print(f"Internal Server Error: {e}")
        except SPARQLExceptions.QueryBadFormed as e:
            # Permanent error: a malformed SPARQL query will never succeed on retry
            print(f"Badly Formed Query: {e}")
            intencore = False
        except SPARQLExceptions.EndPointNotFound as e:
            # Permanent error: a wrong endpoint URL will never succeed on retry
            print(f"Endpoint Not Found: {e}")
            intencore = False
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Honor server-suggested Retry-After when present; default 120s otherwise
                # (WDQS sometimes drops to 1 req/min during outages, so 60s is too tight)
                lngretryafter = 120
                strretryafter = ""
                try:
                    if e.headers is not None:
                        strretryafter = e.headers.get("Retry-After", "") or ""
                except Exception:
                    strretryafter = ""
                if strretryafter:
                    try:
                        lngretryafter = int(strretryafter)
                    except (ValueError, TypeError):
                        lngretryafter = 120
                print(f"HTTP 429 rate-limited by Wikidata. Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
            else:
                print(f"HTTP Error {e.code}: {e}")
                lngretryafter = 60
                print(f"Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
        except Exception as e:
            print(f"An error occurred: {e}")
            lngretryafter = 60
            print(f"Retrying after {lngretryafter} seconds.")
            time.sleep(lngretryafter)

def f_sparqlmoviescrawl(strwikidataidquery,lngyearquery=0):
    global strwikidatauseragent
    global strsparqlmovieinstanceof
    
    strwikidataidprev = ""
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?tmdbID ?releaseDate ?genres ?plexMediaKey ?criterionFilmID ?criterionSpine ?color ?type "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            # Accept either a single Wikidata id or a whitespace-separated list, so callers can batch
            arrwikidataidlist = [s.strip() for s in strwikidataidquery.split() if s.strip()]
            strwikidataidwd = " ".join([f"wd:{s}" for s in arrwikidataidlist])
            strsparqlquery += "VALUES ?item { " + strwikidataidwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P577 ?pubdate. "
        else:
            arrmovieinstanceof = [s.strip() for s in strsparqlmovieinstanceof.split() if s.strip()]
            strmovieinstanceofwd = " ".join([f"wd:{s}" for s in arrmovieinstanceof])
            if strmovieinstanceofwd == "":
                strmovieinstanceofwd = "wd:Q11424"
            strsparqlquery += "VALUES ?type { " + strmovieinstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P577 ?pubdate. "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item DESC(?releaseDate) "
        #strsparqlquery += "LIMIT " + str(lnglimit) + " "
        #strsparqlquery += "OFFSET " + str(lngoffset) + " "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        # Set the query and return format
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        # Execute the query and convert the results
        try:
            query_result = sparql.query()
            results = query_result.convert()
            #print(results)
            # Convert the results to a Pandas DataFrame
            df = pd.json_normalize(results['results']['bindings'])
            lngcount = 0
            if not df.empty:
                #df = df[['item.value', 'itemLabel.value', 'imdbID.value', 'tmdbID.value', 'birthDate.value']]
                for index, row in df.iterrows():
                    lngcount += 1
                    print(row)
                    stritem = row['item.value']
                    # Compute strwikidataid
                    strwikidataid = ""
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlermoviescurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    if strwikidataid != strwikidataidprev:
                        # We process a new movie
                        print("Processing a new movie")
                        #if strwikidataidprev != "":
                        if strwikidataidprev != "":
                            # Now delete genres that are not for the movie we just finished
                            if strgenrelist == "":
                                strgenrelist = "'0'"
                            strpropertyid = "P136"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                            # Now delete colors that are not for the movie we just finished
                            if strcolorlist == "":
                                strcolorlist = "'0'"
                            strpropertyid = "P462"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                        strgenrelist = ""
                        strcolorlist = ""
                        strwikidataidprev = strwikidataid
                    # Compute strtitle
                    strtitle = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strtitle = row['itemLabel.value']
                                # reject any title that looks like a Wikidata ID
                                if re.match(r'^[QPL]\d+$', strtitle):
                                    strtitle = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                	strimdbid = strimdbid[:10]
                    # Compute lngtmdbid
                    lngtmdbid = 0
                    if 'tmdbID.value' in row:
                        if row['tmdbID.value']:
                            if not pd.isna(row['tmdbID.value']):
                                lngtmdbid = row['tmdbID.value']
                                #print(lngtmdbid)
                    # Compute release date
                    strreleasedate = ""
                    strreleasedatesql = ""
                    if 'releaseDate.value' in row:
                        if row['releaseDate.value']:
                            if not pd.isna(row['releaseDate.value']):
                                strreleasedate = row['releaseDate.value']
                                #print(strreleasedate)
                                if strreleasedate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strreleasedate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strreleasedatesql = datreleasedate.strftime("%Y-%m-%d %H:%M:%S")
                                        strreleasedatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strreleasedate} cannot be converted to a date.")
                                        strreleasedatesql = ""
                    # Compute strplexmediakey
                    strplexmediakey = ""
                    if 'plexMediaKey.value' in row:
                        if row['plexMediaKey.value']:
                            if not pd.isna(row['plexMediaKey.value']):
                                strplexmediakey = row['plexMediaKey.value']
                    # Compute lngcriterionfilmid
                    lngcriterionfilmid = 0
                    if 'criterionFilmID.value' in row:
                        if row['criterionFilmID.value']:
                            if not pd.isna(row['criterionFilmID.value']):
                                strcriterionfilmid = row['criterionFilmID.value']
                                #print(strcriterionfilmid)
                                try:
                                    #print(strcriterionfilmid)
                                    lngcriterionfilmid = int(strcriterionfilmid)
                                    #print(f"Converted integer: {lngcriterionfilmid}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionfilmid} cannot be converted to an integer.")
                                    lngcriterionfilmid = 0
                    # Compute lngcriterionspine
                    lngcriterionspine = 0
                    if 'criterionSpine.value' in row:
                        if row['criterionSpine.value']:
                            if not pd.isna(row['criterionSpine.value']):
                                strcriterionspine = row['criterionSpine.value']
                                #print(strcriterionspine)
                                try:
                                    #print(strcriterionspine)
                                    lngcriterionspine = int(strcriterionspine)
                                    #print(f"Converted integer: {lngcriterionspine}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionspine} cannot be converted to an integer.")
                                    lngcriterionspine = 0
                    # Compute strinstanceof
                    strinstanceof = ""
                    strinstanceofid = ""
                    if 'type.value' in row:
                        if row['type.value']:
                            if not pd.isna(row['type.value']):
                                strinstanceof = row['type.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    # Compute strgenre
                    strgenre = ""
                    strgenreid = ""
                    if 'genres.value' in row:
                        if row['genres.value']:
                            if not pd.isna(row['genres.value']):
                                strgenre = row['genres.value']
                                strgenreid = strgenre.split('/')[-1]
                                if strgenre != "":
                                    if strgenrelist != "":
                                        strgenrelist += ","
                                    strgenrelist += "'" + strgenreid + "'"
                                    strpropertyid = "P136"
                                    arrmoviecouples = {}
                                    arrmoviecouples["ID_WIKIDATA"] = strwikidataid
                                    arrmoviecouples["ID_PROPERTY"] = strpropertyid
                                    arrmoviecouples["ID_ITEM"] = strgenreid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strgenreid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                    # Compute strcolor
                    strcolor = ""
                    strcolorid = ""
                    if 'color.value' in row:
                        if row['color.value']:
                            if not pd.isna(row['color.value']):
                                strcolor = row['color.value']
                                strcolorid = strcolor.split('/')[-1]
                                if strcolor != "":
                                    if strcolorlist != "":
                                        strcolorlist += ","
                                    strcolorlist += "'" + strcolorid + "'"
                                    strpropertyid = "P462"
                                    arrmoviecouples = {}
                                    arrmoviecouples["ID_WIKIDATA"] = strwikidataid
                                    arrmoviecouples["ID_PROPERTY"] = strpropertyid
                                    arrmoviecouples["ID_ITEM"] = strcolorid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strcolorid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                    strmessage = f"{strwikidataid} '{strimdbid}' ID {lngtmdbid} '{strtitle}' {strreleasedatesql} Plex: {strplexmediakey} genre: {strgenreid}"
                    print(strmessage)
                    arrmoviecouples = {}
                    arrmoviecouples["ID_WIKIDATA"] = strwikidataid
                    arrmoviecouples["ID_MOVIE"] = lngtmdbid
                    arrmoviecouples["ID_IMDB"] = strimdbid
                    arrmoviecouples["TITLE"] = strtitle
                    if strplexmediakey != "":
                        arrmoviecouples["PLEX_MEDIA_KEY"] = strplexmediakey
                    if strreleasedatesql != "":
                        arrmoviecouples["DAT_RELEASE"] = strreleasedatesql
                    arrmoviecouples["ID_CRITERION"] = lngcriterionfilmid
                    arrmoviecouples["ID_CRITERION_SPINE"] = lngcriterionspine
                    arrmoviecouples["INSTANCE_OF"] = strinstanceofid
                    
                    strsqltablename = "T_WC_WIKIDATA_MOVIE_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrmoviecouples,strsqlupdatecondition,1)
                # End of the loop for the current query so we process the last movie
                if strwikidataidprev != "":
                    # We process a new movie
                    print("Processing the last movie of the query")
                    # Now delete genres that are not for the movie we just finished
                    if strgenrelist == "":
                        strgenrelist = "'0'"
                    strpropertyid = "P136"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strgenrelist = ""
                    # Now delete colors that are not for the movie we just finished
                    if strcolorlist == "":
                        strcolorlist = "'0'"
                    strpropertyid = "P462"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strcolorlist = ""
                    
                    strwikidataidprev = ""
            intencore = False
        except SPARQLExceptions.EndPointInternalError as e:
            print(f"Internal Server Error: {e}")
        except SPARQLExceptions.QueryBadFormed as e:
            # Permanent error: a malformed SPARQL query will never succeed on retry
            print(f"Badly Formed Query: {e}")
            intencore = False
        except SPARQLExceptions.EndPointNotFound as e:
            # Permanent error: a wrong endpoint URL will never succeed on retry
            print(f"Endpoint Not Found: {e}")
            intencore = False
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Honor server-suggested Retry-After when present; default 120s otherwise
                # (WDQS sometimes drops to 1 req/min during outages, so 60s is too tight)
                lngretryafter = 120
                strretryafter = ""
                try:
                    if e.headers is not None:
                        strretryafter = e.headers.get("Retry-After", "") or ""
                except Exception:
                    strretryafter = ""
                if strretryafter:
                    try:
                        lngretryafter = int(strretryafter)
                    except (ValueError, TypeError):
                        lngretryafter = 120
                print(f"HTTP 429 rate-limited by Wikidata. Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
            else:
                print(f"HTTP Error {e.code}: {e}")
                lngretryafter = 60
                print(f"Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
        except Exception as e:
            print(f"An error occurred: {e}")
            lngretryafter = 60
            print(f"Retrying after {lngretryafter} seconds.")
            time.sleep(lngretryafter)

def f_sparqlseriescrawl(strwikidataidquery,lngyearquery=0):
    global strwikidatauseragent
    global strsparqlserieinstanceof
    
    strwikidataidprev = ""
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?tmdbID ?startTime ?endTime ?genres ?plexMediaKey ?criterionFilmID ?criterionSpine ?color ?type "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            # Accept either a single Wikidata id or a whitespace-separated list, so callers can batch
            arrwikidataidlist = [s.strip() for s in strwikidataidquery.split() if s.strip()]
            strwikidataidwd = " ".join([f"wd:{s}" for s in arrwikidataidlist])
            strsparqlquery += "VALUES ?item { " + strwikidataidwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P580 ?pubdate. "
        else:
            arrserieinstanceof = [s.strip() for s in strsparqlserieinstanceof.split() if s.strip()]
            strserieinstanceofwd = " ".join([f"wd:{s}" for s in arrserieinstanceof])
            if strserieinstanceofwd == "":
                strserieinstanceofwd = "wd:Q5398426"
            strsparqlquery += "VALUES ?type { " + strserieinstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P4947 ?tmdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P136 ?genres. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P11460 ?plexMediaKey. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P9584 ?criterionFilmID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P12279 ?criterionSpine. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P462 ?color. } "
            strsparqlquery += "?item wdt:P580 ?pubdate. "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item DESC(?startTime) "
        #strsparqlquery += "LIMIT " + str(lnglimit) + " "
        #strsparqlquery += "OFFSET " + str(lngoffset) + " "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        # Set the query and return format
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        # Execute the query and convert the results
        try:
            query_result = sparql.query()
            results = query_result.convert()
            #print(results)
            # Convert the results to a Pandas DataFrame
            df = pd.json_normalize(results['results']['bindings'])
            lngcount = 0
            if not df.empty:
                #df = df[['item.value', 'itemLabel.value', 'imdbID.value', 'tmdbID.value', 'birthDate.value']]
                for index, row in df.iterrows():
                    lngcount += 1
                    print(row)
                    stritem = row['item.value']
                    # Compute strwikidataid
                    strwikidataid = ""
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlerseriescurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    if strwikidataid != strwikidataidprev:
                        # We process a new serie
                        print("Processing a new serie")
                        #if strwikidataidprev != "":
                        if strwikidataidprev != "":
                            # Now delete genres that are not for the serie we just finished
                            if strgenrelist == "":
                                strgenrelist = "'0'"
                            strpropertyid = "P136"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                            # Now delete colors that are not for the serie we just finished
                            if strcolorlist == "":
                                strcolorlist = "'0'"
                            strpropertyid = "P462"
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                            print(f"{strsqldelete}")
                            cursor3.execute(strsqldelete)
                            cp.connectioncp.commit()
                        strgenrelist = ""
                        strcolorlist = ""
                        strwikidataidprev = strwikidataid
                    # Compute strtitle
                    strtitle = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strtitle = row['itemLabel.value']
                                # reject any title that looks like a Wikidata ID
                                if re.match(r'^[QPL]\d+$', strtitle):
                                    strtitle = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                	strimdbid = strimdbid[:10]
                    # Compute lngtmdbid
                    lngtmdbid = 0
                    if 'tmdbID.value' in row:
                        if row['tmdbID.value']:
                            if not pd.isna(row['tmdbID.value']):
                                lngtmdbid = row['tmdbID.value']
                                #print(lngtmdbid)
                    # Compute start date
                    strstartdate = ""
                    strstartdatesql = ""
                    if 'startTime.value' in row:
                        if row['startTime.value']:
                            if not pd.isna(row['startTime.value']):
                                strstartdate = row['startTime.value']
                                #print(strstartdate)
                                if strstartdate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strstartdate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strstartdatesql = datreleasedate.strftime("%Y-%m-%d %H:%M:%S")
                                        strstartdatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strstartdate} cannot be converted to a date.")
                                        strstartdatesql = ""
                    # Compute end date
                    strenddate = ""
                    strenddatesql = ""
                    if 'endTime.value' in row:
                        if row['endTime.value']:
                            if not pd.isna(row['endTime.value']):
                                strenddate = row['endTime.value']
                                #print(strenddate)
                                if strenddate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strenddate, "%Y-%m-%dT%H:%M:%SZ")
                                        #strenddatesql = datreleasedate.strftime("%Y-%m-%d %H:%M:%S")
                                        strenddatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        # Handle the case where the string cannot be converted to a date
                                        #print(f"Invalid input: {strenddate} cannot be converted to a date.")
                                        strenddatesql = ""
                    # Compute strplexmediakey
                    strplexmediakey = ""
                    if 'plexMediaKey.value' in row:
                        if row['plexMediaKey.value']:
                            if not pd.isna(row['plexMediaKey.value']):
                                strplexmediakey = row['plexMediaKey.value']
                    # Compute lngcriterionfilmid
                    lngcriterionfilmid = 0
                    if 'criterionFilmID.value' in row:
                        if row['criterionFilmID.value']:
                            if not pd.isna(row['criterionFilmID.value']):
                                strcriterionfilmid = row['criterionFilmID.value']
                                #print(strcriterionfilmid)
                                try:
                                    #print(strcriterionfilmid)
                                    lngcriterionfilmid = int(strcriterionfilmid)
                                    #print(f"Converted integer: {lngcriterionfilmid}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionfilmid} cannot be converted to an integer.")
                                    lngcriterionfilmid = 0
                    # Compute lngcriterionspine
                    lngcriterionspine = 0
                    if 'criterionSpine.value' in row:
                        if row['criterionSpine.value']:
                            if not pd.isna(row['criterionSpine.value']):
                                strcriterionspine = row['criterionSpine.value']
                                #print(strcriterionspine)
                                try:
                                    #print(strcriterionspine)
                                    lngcriterionspine = int(strcriterionspine)
                                    #print(f"Converted integer: {lngcriterionspine}")
                                except ValueError:
                                    # Handle the case where the string cannot be converted to an integer
                                    #print(f"Invalid input: {strcriterionspine} cannot be converted to an integer.")
                                    lngcriterionspine = 0
                    # Compute strinstanceof
                    strinstanceof = ""
                    strinstanceofid = ""
                    if 'type.value' in row:
                        if row['type.value']:
                            if not pd.isna(row['type.value']):
                                strinstanceof = row['type.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    # Compute strgenre
                    strgenre = ""
                    strgenreid = ""
                    if 'genres.value' in row:
                        if row['genres.value']:
                            if not pd.isna(row['genres.value']):
                                strgenre = row['genres.value']
                                strgenreid = strgenre.split('/')[-1]
                                if strgenre != "":
                                    if strgenrelist != "":
                                        strgenrelist += ","
                                    strgenrelist += "'" + strgenreid + "'"
                                    strpropertyid = "P136"
                                    arrseriecouples = {}
                                    arrseriecouples["ID_WIKIDATA"] = strwikidataid
                                    arrseriecouples["ID_PROPERTY"] = strpropertyid
                                    arrseriecouples["ID_ITEM"] = strgenreid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strgenreid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)
                    # Compute strcolor
                    strcolor = ""
                    strcolorid = ""
                    if 'color.value' in row:
                        if row['color.value']:
                            if not pd.isna(row['color.value']):
                                strcolor = row['color.value']
                                strcolorid = strcolor.split('/')[-1]
                                if strcolor != "":
                                    if strcolorlist != "":
                                        strcolorlist += ","
                                    strcolorlist += "'" + strcolorid + "'"
                                    strpropertyid = "P462"
                                    arrseriecouples = {}
                                    arrseriecouples["ID_WIKIDATA"] = strwikidataid
                                    arrseriecouples["ID_PROPERTY"] = strpropertyid
                                    arrseriecouples["ID_ITEM"] = strcolorid
                                    strsqltablename = "T_WC_WIKIDATA_ITEM_PROPERTY"
                                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}' AND ID_PROPERTY = '{strpropertyid}' AND ID_ITEM = '{strcolorid}'"
                                    cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)
                    strmessage = f"{strwikidataid} '{strimdbid}' ID {lngtmdbid} '{strtitle}' {strstartdatesql}-{strenddatesql} Plex: {strplexmediakey} genre: {strgenreid}"
                    print(strmessage)
                    arrseriecouples = {}
                    arrseriecouples["ID_WIKIDATA"] = strwikidataid
                    arrseriecouples["ID_SERIE"] = lngtmdbid
                    arrseriecouples["ID_IMDB"] = strimdbid
                    arrseriecouples["TITLE"] = strtitle
                    if strplexmediakey != "":
                        arrseriecouples["PLEX_MEDIA_KEY"] = strplexmediakey
                    if strstartdatesql != "":
                        arrseriecouples["DAT_START"] = strstartdatesql
                    if strenddatesql != "":
                        arrseriecouples["DAT_END"] = strenddatesql
                    arrseriecouples["ID_CRITERION"] = lngcriterionfilmid
                    arrseriecouples["ID_CRITERION_SPINE"] = lngcriterionspine
                    arrseriecouples["INSTANCE_OF"] = strinstanceofid
                    
                    strsqltablename = "T_WC_WIKIDATA_SERIE_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrseriecouples,strsqlupdatecondition,1)
                # End of the loop for the current query so we process the last serie
                if strwikidataidprev != "":
                    # We process a new serie
                    print("Processing the last serie of the query")
                    # Now delete genres that are not for the serie we just finished
                    if strgenrelist == "":
                        strgenrelist = "'0'"
                    strpropertyid = "P136"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strgenrelist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strgenrelist = ""
                    # Now delete colors that are not for the serie we just finished
                    if strcolorlist == "":
                        strcolorlist = "'0'"
                    strpropertyid = "P462"
                    strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_PROPERTY WHERE ID_WIKIDATA = '" + strwikidataidprev + "' AND ID_PROPERTY = '" + strpropertyid + "' AND ID_ITEM NOT IN (" + strcolorlist + ")"
                    print(f"{strsqldelete}")
                    cursor3.execute(strsqldelete)
                    cp.connectioncp.commit()
                    strcolorlist = ""
                    
                    strwikidataidprev = ""
            intencore = False
        except SPARQLExceptions.EndPointInternalError as e:
            print(f"Internal Server Error: {e}")
        except SPARQLExceptions.QueryBadFormed as e:
            # Permanent error: a malformed SPARQL query will never succeed on retry
            print(f"Badly Formed Query: {e}")
            intencore = False
        except SPARQLExceptions.EndPointNotFound as e:
            # Permanent error: a wrong endpoint URL will never succeed on retry
            print(f"Endpoint Not Found: {e}")
            intencore = False
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Honor server-suggested Retry-After when present; default 120s otherwise
                # (WDQS sometimes drops to 1 req/min during outages, so 60s is too tight)
                lngretryafter = 120
                strretryafter = ""
                try:
                    if e.headers is not None:
                        strretryafter = e.headers.get("Retry-After", "") or ""
                except Exception:
                    strretryafter = ""
                if strretryafter:
                    try:
                        lngretryafter = int(strretryafter)
                    except (ValueError, TypeError):
                        lngretryafter = 120
                print(f"HTTP 429 rate-limited by Wikidata. Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
            else:
                print(f"HTTP Error {e.code}: {e}")
                lngretryafter = 60
                print(f"Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
        except Exception as e:
            print(f"An error occurred: {e}")
            lngretryafter = 60
            print(f"Retrying after {lngretryafter} seconds.")
            time.sleep(lngretryafter)

def f_sparqlseasonscrawl(strwikidataidquery,lngyearquery=0,strseriewikidataidquery=""):
    global strwikidatauseragent
    global strsparqlseasoninstanceof

    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?seriesItem ?seasonNumber ?startTime ?endTime ?type "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            arrwikidataidlist = [s.strip() for s in strwikidataidquery.split() if s.strip()]
            strwikidataidwd = " ".join([f"wd:{s}" for s in arrwikidataidlist])
            strsparqlquery += "VALUES ?item { " + strwikidataidwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
            strsparqlquery += "OPTIONAL { ?item p:P179 ?seriesStatement. ?seriesStatement ps:P179 ?seriesItem. OPTIONAL { ?seriesStatement pq:P1545 ?seasonNumber. } } "
        elif strseriewikidataidquery != "":
            # Discover seasons by walking back from a batch of parent series (P179 backlink).
            # Catches seasons missing P580 that the year-driven crawl skips entirely.
            arrserieidlist = [s.strip() for s in strseriewikidataidquery.split() if s.strip()]
            strserieidwd = " ".join([f"wd:{s}" for s in arrserieidlist])
            arrseasoninstanceof = [s.strip() for s in strsparqlseasoninstanceof.split() if s.strip()]
            strseasoninstanceofwd = " ".join([f"wd:{s}" for s in arrseasoninstanceof])
            if strseasoninstanceofwd == "":
                strseasoninstanceofwd = "wd:Q3464665"
            strsparqlquery += "VALUES ?type { " + strseasoninstanceofwd + " } "
            strsparqlquery += "VALUES ?seriesItem { " + strserieidwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "?item p:P179 ?seriesStatement. ?seriesStatement ps:P179 ?seriesItem. "
            strsparqlquery += "OPTIONAL { ?seriesStatement pq:P1545 ?seasonNumber. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
        else:
            arrseasoninstanceof = [s.strip() for s in strsparqlseasoninstanceof.split() if s.strip()]
            strseasoninstanceofwd = " ".join([f"wd:{s}" for s in arrseasoninstanceof])
            if strseasoninstanceofwd == "":
                strseasoninstanceofwd = "wd:Q3464665"
            strsparqlquery += "VALUES ?type { " + strseasoninstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P580 ?startTime. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P582 ?endTime. } "
            strsparqlquery += "OPTIONAL { ?item p:P179 ?seriesStatement. ?seriesStatement ps:P179 ?seriesItem. OPTIONAL { ?seriesStatement pq:P1545 ?seasonNumber. } } "
            strsparqlquery += "?item wdt:P580 ?pubdate. "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item DESC(?startTime) "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        try:
            query_result = sparql.query()
            results = query_result.convert()
            df = pd.json_normalize(results['results']['bindings'])
            if not df.empty:
                for index, row in df.iterrows():
                    print(row)
                    stritem = row['item.value']
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlerseasonscurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    # Compute strtitle
                    strtitle = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strtitle = row['itemLabel.value']
                                if re.match(r'^[QPL]\d+$', strtitle):
                                    strtitle = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                    strimdbid = strimdbid[:10]
                    # Compute start date
                    strstartdatesql = ""
                    if 'startTime.value' in row:
                        if row['startTime.value']:
                            if not pd.isna(row['startTime.value']):
                                strstartdate = row['startTime.value']
                                if strstartdate != "":
                                    try:
                                        datstartdate = datetime.strptime(strstartdate, "%Y-%m-%dT%H:%M:%SZ")
                                        strstartdatesql = datstartdate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        strstartdatesql = ""
                    # Compute end date
                    strenddatesql = ""
                    if 'endTime.value' in row:
                        if row['endTime.value']:
                            if not pd.isna(row['endTime.value']):
                                strenddate = row['endTime.value']
                                if strenddate != "":
                                    try:
                                        datenddate = datetime.strptime(strenddate, "%Y-%m-%dT%H:%M:%SZ")
                                        strenddatesql = datenddate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        strenddatesql = ""
                    # Compute strinstanceof
                    strinstanceofid = ""
                    if 'type.value' in row:
                        if row['type.value']:
                            if not pd.isna(row['type.value']):
                                strinstanceof = row['type.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    # Compute parent series Wikidata id
                    strseriewikidataid = ""
                    if 'seriesItem.value' in row:
                        if row['seriesItem.value']:
                            if not pd.isna(row['seriesItem.value']):
                                strseriewikidataid = row['seriesItem.value'].split('/')[-1]
                    # Compute season number (P1545 qualifier of P179)
                    lngseasonnumber = -1
                    if 'seasonNumber.value' in row:
                        if row['seasonNumber.value']:
                            if not pd.isna(row['seasonNumber.value']):
                                try:
                                    lngseasonnumber = int(row['seasonNumber.value'])
                                except (ValueError, TypeError):
                                    lngseasonnumber = -1
                    # Lookup TMDb ID_SERIE from T_WC_WIKIDATA_SERIE_V1 for the parent series
                    lngserietmdbid = 0
                    if strseriewikidataid != "":
                        cursor3.execute(f"SELECT ID_SERIE FROM T_WC_WIKIDATA_SERIE_V1 WHERE ID_WIKIDATA = '{strseriewikidataid}' LIMIT 1")
                        rowserie = cursor3.fetchone()
                        if rowserie and rowserie.get('ID_SERIE'):
                            lngserietmdbid = rowserie['ID_SERIE']
                    strmessage = f"{strwikidataid} '{strimdbid}' '{strtitle}' S{lngseasonnumber} {strstartdatesql}-{strenddatesql} parent serie {strseriewikidataid}/{lngserietmdbid}"
                    print(strmessage)
                    arrseasoncouples = {}
                    arrseasoncouples["ID_WIKIDATA"] = strwikidataid
                    arrseasoncouples["ID_SEASON"] = 0
                    arrseasoncouples["TITLE"] = strtitle
                    if strimdbid != "":
                        arrseasoncouples["ID_IMDB"] = strimdbid
                    if strstartdatesql != "":
                        arrseasoncouples["DAT_START"] = strstartdatesql
                    if strenddatesql != "":
                        arrseasoncouples["DAT_END"] = strenddatesql
                    if strseriewikidataid != "":
                        arrseasoncouples["ID_WIKIDATA_SERIE"] = strseriewikidataid
                    if lngserietmdbid:
                        arrseasoncouples["ID_SERIE"] = lngserietmdbid
                    if lngseasonnumber >= 0:
                        arrseasoncouples["SEASON_NUMBER"] = lngseasonnumber
                    arrseasoncouples["INSTANCE_OF"] = strinstanceofid

                    strsqltablename = "T_WC_WIKIDATA_SEASON_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrseasoncouples,strsqlupdatecondition,1)
            intencore = False
        except SPARQLExceptions.EndPointInternalError as e:
            print(f"Internal Server Error: {e}")
        except SPARQLExceptions.QueryBadFormed as e:
            print(f"Badly Formed Query: {e}")
            intencore = False
        except SPARQLExceptions.EndPointNotFound as e:
            print(f"Endpoint Not Found: {e}")
            intencore = False
        except urllib.error.HTTPError as e:
            if e.code == 429:
                lngretryafter = 120
                strretryafter = ""
                try:
                    if e.headers is not None:
                        strretryafter = e.headers.get("Retry-After", "") or ""
                except Exception:
                    strretryafter = ""
                if strretryafter:
                    try:
                        lngretryafter = int(strretryafter)
                    except (ValueError, TypeError):
                        lngretryafter = 120
                print(f"HTTP 429 rate-limited by Wikidata. Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
            else:
                print(f"HTTP Error {e.code}: {e}")
                lngretryafter = 60
                print(f"Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
        except Exception as e:
            print(f"An error occurred: {e}")
            lngretryafter = 60
            print(f"Retrying after {lngretryafter} seconds.")
            time.sleep(lngretryafter)

def f_sparqlepisodescrawl(strwikidataidquery,lngyearquery=0,strseriewikidataidquery=""):
    global strwikidatauseragent
    global strsparqlepisodeinstanceof

    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?releaseDate ?seriesItem ?seasonItem ?episodeNumber ?type "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            arrwikidataidlist = [s.strip() for s in strwikidataidquery.split() if s.strip()]
            strwikidataidwd = " ".join([f"wd:{s}" for s in arrwikidataidlist])
            strsparqlquery += "VALUES ?item { " + strwikidataidwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P179 ?seriesItem. } "
            strsparqlquery += "OPTIONAL { ?item p:P4908 ?seasonStatement. ?seasonStatement ps:P4908 ?seasonItem. OPTIONAL { ?seasonStatement pq:P1545 ?episodeNumber. } } "
        elif strseriewikidataidquery != "":
            # Discover episodes by walking back from a batch of parent series. Two paths in Wikidata:
            #   A) episode --P4908--> season --P179--> series   (modern multi-season shows)
            #   B) episode --P179--> series directly             (single-season or older data)
            # UNION covers both. Duplicates across the two arms are idempotent under f_sqlupdatearray.
            # Triple order matters: each arm starts from the series-anchored triple so Blazegraph
            # drives the join from the small VALUES ?seriesItem set instead of enumerating every
            # TV episode in Wikidata. The wdt:P31 ?type check is intentionally placed last inside
            # each arm so the optimizer cannot pick it as the leading (catastrophic) join.
            arrserieidlist = [s.strip() for s in strseriewikidataidquery.split() if s.strip()]
            strserieidwd = " ".join([f"wd:{s}" for s in arrserieidlist])
            arrepisodeinstanceof = [s.strip() for s in strsparqlepisodeinstanceof.split() if s.strip()]
            strepisodeinstanceofwd = " ".join([f"wd:{s}" for s in arrepisodeinstanceof])
            if strepisodeinstanceofwd == "":
                strepisodeinstanceofwd = "wd:Q21191270"
            strsparqlquery += "VALUES ?type { " + strepisodeinstanceofwd + " } "
            strsparqlquery += "VALUES ?seriesItem { " + strserieidwd + " } "
            strsparqlquery += "{ "
            strsparqlquery += "?seasonItem wdt:P179 ?seriesItem. "
            strsparqlquery += "?item p:P4908 ?seasonStatement. ?seasonStatement ps:P4908 ?seasonItem. "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?seasonStatement pq:P1545 ?episodeNumber. } "
            strsparqlquery += "} UNION { "
            strsparqlquery += "?item p:P179 ?seriesStatement. ?seriesStatement ps:P179 ?seriesItem. "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?seriesStatement pq:P1545 ?episodeNumber. } "
            strsparqlquery += "} "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
        else:
            arrepisodeinstanceof = [s.strip() for s in strsparqlepisodeinstanceof.split() if s.strip()]
            strepisodeinstanceofwd = " ".join([f"wd:{s}" for s in arrepisodeinstanceof])
            if strepisodeinstanceofwd == "":
                strepisodeinstanceofwd = "wd:Q21191270"
            strsparqlquery += "VALUES ?type { " + strepisodeinstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?type. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P577 ?releaseDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P179 ?seriesItem. } "
            strsparqlquery += "OPTIONAL { ?item p:P4908 ?seasonStatement. ?seasonStatement ps:P4908 ?seasonItem. OPTIONAL { ?seasonStatement pq:P1545 ?episodeNumber. } } "
            strsparqlquery += "?item wdt:P577 ?pubdate. "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item DESC(?releaseDate) "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        try:
            query_result = sparql.query()
            results = query_result.convert()
            df = pd.json_normalize(results['results']['bindings'])
            if not df.empty:
                for index, row in df.iterrows():
                    print(row)
                    stritem = row['item.value']
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlerepisodescurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    # Compute strtitle
                    strtitle = ""
                    if 'itemLabel.value' in row:
                        if row['itemLabel.value']:
                            if not pd.isna(row['itemLabel.value']):
                                strtitle = row['itemLabel.value']
                                if re.match(r'^[QPL]\d+$', strtitle):
                                    strtitle = ""
                    # Compute strimdbid
                    strimdbid = ""
                    if 'imdbID.value' in row:
                        if row['imdbID.value']:
                            if not pd.isna(row['imdbID.value']):
                                strimdbid = row['imdbID.value']
                                if len(strimdbid) > 10:
                                    strimdbid = strimdbid[:10]
                    # Compute release date
                    strreleasedatesql = ""
                    if 'releaseDate.value' in row:
                        if row['releaseDate.value']:
                            if not pd.isna(row['releaseDate.value']):
                                strreleasedate = row['releaseDate.value']
                                if strreleasedate != "":
                                    try:
                                        datreleasedate = datetime.strptime(strreleasedate, "%Y-%m-%dT%H:%M:%SZ")
                                        strreleasedatesql = datreleasedate.strftime("%Y-%m-%d")
                                    except ValueError:
                                        strreleasedatesql = ""
                    # Compute strinstanceof
                    strinstanceofid = ""
                    if 'type.value' in row:
                        if row['type.value']:
                            if not pd.isna(row['type.value']):
                                strinstanceof = row['type.value']
                                strinstanceofid = strinstanceof.split('/')[-1]
                    # Compute parent series Wikidata id
                    strseriewikidataid = ""
                    if 'seriesItem.value' in row:
                        if row['seriesItem.value']:
                            if not pd.isna(row['seriesItem.value']):
                                strseriewikidataid = row['seriesItem.value'].split('/')[-1]
                    # Compute parent season Wikidata id
                    strseasonwikidataid = ""
                    if 'seasonItem.value' in row:
                        if row['seasonItem.value']:
                            if not pd.isna(row['seasonItem.value']):
                                strseasonwikidataid = row['seasonItem.value'].split('/')[-1]
                    # Compute episode number (P1545 qualifier of P4908)
                    lngepisodenumber = -1
                    if 'episodeNumber.value' in row:
                        if row['episodeNumber.value']:
                            if not pd.isna(row['episodeNumber.value']):
                                try:
                                    lngepisodenumber = int(row['episodeNumber.value'])
                                except (ValueError, TypeError):
                                    lngepisodenumber = -1
                    # Lookup parent season row for ID_SEASON, SEASON_NUMBER and ID_WIKIDATA_SERIE fallback
                    lngseasontmdbid = 0
                    lngseasonnumber = -1
                    if strseasonwikidataid != "":
                        cursor3.execute(f"SELECT ID_SEASON, SEASON_NUMBER, ID_WIKIDATA_SERIE FROM T_WC_WIKIDATA_SEASON_V1 WHERE ID_WIKIDATA = '{strseasonwikidataid}' LIMIT 1")
                        rowseason = cursor3.fetchone()
                        if rowseason:
                            if rowseason.get('ID_SEASON'):
                                lngseasontmdbid = rowseason['ID_SEASON']
                            if rowseason.get('SEASON_NUMBER') is not None:
                                lngseasonnumber = rowseason['SEASON_NUMBER']
                            if strseriewikidataid == "" and rowseason.get('ID_WIKIDATA_SERIE'):
                                strseriewikidataid = rowseason['ID_WIKIDATA_SERIE']
                    # Lookup TMDb ID_SERIE from T_WC_WIKIDATA_SERIE_V1 for the parent series
                    lngserietmdbid = 0
                    if strseriewikidataid != "":
                        cursor3.execute(f"SELECT ID_SERIE FROM T_WC_WIKIDATA_SERIE_V1 WHERE ID_WIKIDATA = '{strseriewikidataid}' LIMIT 1")
                        rowserie = cursor3.fetchone()
                        if rowserie and rowserie.get('ID_SERIE'):
                            lngserietmdbid = rowserie['ID_SERIE']
                    strmessage = f"{strwikidataid} '{strimdbid}' '{strtitle}' S{lngseasonnumber}E{lngepisodenumber} {strreleasedatesql} parent serie {strseriewikidataid}/{lngserietmdbid} season {strseasonwikidataid}/{lngseasontmdbid}"
                    print(strmessage)
                    arrepisodecouples = {}
                    arrepisodecouples["ID_WIKIDATA"] = strwikidataid
                    arrepisodecouples["ID_EPISODE"] = 0
                    arrepisodecouples["TITLE"] = strtitle
                    if strimdbid != "":
                        arrepisodecouples["ID_IMDB"] = strimdbid
                    if strreleasedatesql != "":
                        arrepisodecouples["DAT_RELEASE"] = strreleasedatesql
                    if strseriewikidataid != "":
                        arrepisodecouples["ID_WIKIDATA_SERIE"] = strseriewikidataid
                    if lngserietmdbid:
                        arrepisodecouples["ID_SERIE"] = lngserietmdbid
                    if strseasonwikidataid != "":
                        arrepisodecouples["ID_WIKIDATA_SEASON"] = strseasonwikidataid
                    if lngseasontmdbid:
                        arrepisodecouples["ID_SEASON"] = lngseasontmdbid
                    if lngseasonnumber >= 0:
                        arrepisodecouples["SEASON_NUMBER"] = lngseasonnumber
                    if lngepisodenumber >= 0:
                        arrepisodecouples["EPISODE_NUMBER"] = lngepisodenumber
                    arrepisodecouples["INSTANCE_OF"] = strinstanceofid

                    strsqltablename = "T_WC_WIKIDATA_EPISODE_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataid}'"
                    cp.f_sqlupdatearray(strsqltablename,arrepisodecouples,strsqlupdatecondition,1)
            intencore = False
        except SPARQLExceptions.EndPointInternalError as e:
            print(f"Internal Server Error: {e}")
        except SPARQLExceptions.QueryBadFormed as e:
            print(f"Badly Formed Query: {e}")
            intencore = False
        except SPARQLExceptions.EndPointNotFound as e:
            print(f"Endpoint Not Found: {e}")
            intencore = False
        except urllib.error.HTTPError as e:
            if e.code == 429:
                lngretryafter = 120
                strretryafter = ""
                try:
                    if e.headers is not None:
                        strretryafter = e.headers.get("Retry-After", "") or ""
                except Exception:
                    strretryafter = ""
                if strretryafter:
                    try:
                        lngretryafter = int(strretryafter)
                    except (ValueError, TypeError):
                        lngretryafter = 120
                print(f"HTTP 429 rate-limited by Wikidata. Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
            else:
                print(f"HTTP Error {e.code}: {e}")
                lngretryafter = 60
                print(f"Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
        except Exception as e:
            print(f"An error occurred: {e}")
            lngretryafter = 60
            print(f"Retrying after {lngretryafter} seconds.")
            time.sleep(lngretryafter)

def f_sparqlcharactercrawl(strwikidataidquery,lngyearquery=0,strworkwikidataidquery="",strpersonwikidataidquery=""):
    global strwikidatauseragent
    global strsparqlcharacterinstanceof

    strwikidataidprev = ""
    arrcurrentdata = {}
    arrcurrentaliases = []
    intencore = True
    while intencore:
        strsparqlquery = ""
        strsparqlquery += "SELECT ?item ?itemLabel ?imdbID ?birthDate ?deathDate ?instanceOf ?aliasLabel "
        strsparqlquery += "WHERE { "
        if strwikidataidquery != "":
            # Accept either a single Wikidata id or a whitespace-separated list, so callers can batch
            arrwikidataidlist = [s.strip() for s in strwikidataidquery.split() if s.strip()]
            strwikidataidwd = " ".join([f"wd:{s}" for s in arrwikidataidlist])
            strsparqlquery += "VALUES ?item { " + strwikidataidwd + " } "
            strsparqlquery += "?item wdt:P31 ?instanceOf. "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P569 ?birthDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
            strsparqlquery += "OPTIONAL { ?item skos:altLabel ?aliasLabel. FILTER(LANG(?aliasLabel) = \"en\") } "
        elif strworkwikidataidquery != "":
            # Discover characters by walking back from a batch of works (films + series). Two paths in Wikidata:
            #   A) work --P674--> character                              (work directly lists its characters)
            #   B) work --P161-->[cast statement]--pq:P453--> character  (cast member statement with character-role qualifier)
            # P31 is OPTIONAL here: if the entity is the object of P674 or P453 it is a character, regardless of its declared instance-of.
            arrworkidlist = [s.strip() for s in strworkwikidataidquery.split() if s.strip()]
            strworkidwd = " ".join([f"wd:{s}" for s in arrworkidlist])
            strsparqlquery += "VALUES ?work { " + strworkidwd + " } "
            strsparqlquery += "{ ?work wdt:P674 ?item. } UNION { ?work p:P161 ?castStatement. ?castStatement pq:P453 ?item. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P31 ?instanceOf. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P569 ?birthDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
            strsparqlquery += "OPTIONAL { ?item skos:altLabel ?aliasLabel. FILTER(LANG(?aliasLabel) = \"en\") } "
        elif strpersonwikidataidquery != "":
            # Discover characters by walking back from a batch of persons (actors): every work where the person
            # is a cast member (P161) with a character-role qualifier (P453) yields the character on that role.
            # Catches secondary / one-off roles that no work explicitly lists in P674.
            arrpersonidlist = [s.strip() for s in strpersonwikidataidquery.split() if s.strip()]
            strpersonidwd = " ".join([f"wd:{s}" for s in arrpersonidlist])
            strsparqlquery += "VALUES ?person { " + strpersonidwd + " } "
            strsparqlquery += "?work p:P161 ?castStatement. ?castStatement ps:P161 ?person. ?castStatement pq:P453 ?item. "
            strsparqlquery += "OPTIONAL { ?item wdt:P31 ?instanceOf. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P569 ?birthDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
            strsparqlquery += "OPTIONAL { ?item skos:altLabel ?aliasLabel. FILTER(LANG(?aliasLabel) = \"en\") } "
        else:
            arrcharacterinstanceof = [s.strip() for s in strsparqlcharacterinstanceof.split() if s.strip()]
            strcharacterinstanceofwd = " ".join([f"wd:{s}" for s in arrcharacterinstanceof])
            if strcharacterinstanceofwd == "":
                strcharacterinstanceofwd = "wd:Q15632617"
            strsparqlquery += "VALUES ?instanceOf { " + strcharacterinstanceofwd + " } "
            strsparqlquery += "?item wdt:P31 ?instanceOf. "
            # Many fictional characters carry P4584 (date of first appearance) but not P577 (publication date).
            # Accept either as the year-anchor so the year-driven loop reaches the wider catalogue;
            # the FILTER below still applies to whichever date got bound.
            strsparqlquery += "{ ?item wdt:P577 ?pubdate. } UNION { ?item wdt:P4584 ?pubdate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P345 ?imdbID. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P569 ?birthDate. } "
            strsparqlquery += "OPTIONAL { ?item wdt:P570 ?deathDate. } "
            strsparqlquery += "OPTIONAL { ?item skos:altLabel ?aliasLabel. FILTER(LANG(?aliasLabel) = \"en\") } "
            if lngyearquery > 0:
                strsparqlquery += "FILTER((?pubdate >= \"" + str(lngyearquery) + "-01-01T00:00:00Z\"^^xsd:dateTime) && (?pubdate <= \"" + str(lngyearquery) + "-12-31T00:00:00Z\"^^xsd:dateTime)) "
        strsparqlquery += "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],mul,en\". } "
        strsparqlquery += "} "
        strsparqlquery += "ORDER BY ?item "
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=strwikidatauseragent)
        print(strsparqlquery)
        sparql.setQuery(strsparqlquery)
        sparql.setReturnFormat(JSON)
        sparql.setMethod(POST)
        try:
            query_result = sparql.query()
            results = query_result.convert()
            df = pd.json_normalize(results['results']['bindings'])
            if not df.empty:
                for index, row in df.iterrows():
                    stritem = row['item.value']
                    strwikidataid = stritem.split('/')[-1]
                    cp.f_setservervariable("strsparqlaltcrawlercharacterscurrentvalue",strwikidataid,"Current value in the current Wikidata SPARQL alternative crawler",0)
                    if strwikidataid != strwikidataidprev:
                        # Flush the previous character before starting the new one
                        if strwikidataidprev != "":
                            if arrcurrentaliases:
                                arrcurrentdata["ALIASES"] = ", ".join(arrcurrentaliases)
                            strsqltablename = "T_WC_WIKIDATA_CHARACTER_V1"
                            strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataidprev}'"
                            cp.f_sqlupdatearray(strsqltablename,arrcurrentdata,strsqlupdatecondition,1)
                        arrcurrentdata = {}
                        arrcurrentaliases = []
                        strwikidataidprev = strwikidataid
                        # Initialize scalar fields from this row (NAME required, rest optional)
                        arrcurrentdata["ID_WIKIDATA"] = strwikidataid
                        # Compute strname (only required field)
                        strname = ""
                        if 'itemLabel.value' in row:
                            if row['itemLabel.value']:
                                if not pd.isna(row['itemLabel.value']):
                                    strname = row['itemLabel.value']
                                    # reject any name that looks like a Wikidata ID
                                    if re.match(r'^[QPL]\d+$', strname):
                                        strname = ""
                        arrcurrentdata["NAME"] = strname
                        # Compute strimdbid
                        strimdbid = ""
                        if 'imdbID.value' in row:
                            if row['imdbID.value']:
                                if not pd.isna(row['imdbID.value']):
                                    strimdbid = row['imdbID.value']
                                    if len(strimdbid) > 10:
                                        strimdbid = strimdbid[:10]
                        if strimdbid != "":
                            arrcurrentdata["ID_IMDB"] = strimdbid
                        # Compute birth date
                        strbirthdatesql = ""
                        if 'birthDate.value' in row:
                            if row['birthDate.value']:
                                if not pd.isna(row['birthDate.value']):
                                    strbirthdate = row['birthDate.value']
                                    if strbirthdate != "":
                                        try:
                                            datbirthdate = datetime.strptime(strbirthdate, "%Y-%m-%dT%H:%M:%SZ")
                                            strbirthdatesql = datbirthdate.strftime("%Y-%m-%d")
                                        except ValueError:
                                            strbirthdatesql = ""
                        if strbirthdatesql != "":
                            arrcurrentdata["BIRTHDAY"] = strbirthdatesql
                        # Compute death date
                        strdeathdatesql = ""
                        if 'deathDate.value' in row:
                            if row['deathDate.value']:
                                if not pd.isna(row['deathDate.value']):
                                    strdeathdate = row['deathDate.value']
                                    if strdeathdate != "":
                                        try:
                                            datdeathdate = datetime.strptime(strdeathdate, "%Y-%m-%dT%H:%M:%SZ")
                                            strdeathdatesql = datdeathdate.strftime("%Y-%m-%d")
                                        except ValueError:
                                            strdeathdatesql = ""
                        if strdeathdatesql != "":
                            arrcurrentdata["DEATHDAY"] = strdeathdatesql
                        # Compute instance of
                        strinstanceofid = ""
                        if 'instanceOf.value' in row:
                            if row['instanceOf.value']:
                                if not pd.isna(row['instanceOf.value']):
                                    strinstanceof = row['instanceOf.value']
                                    strinstanceofid = strinstanceof.split('/')[-1]
                        if strinstanceofid != "":
                            arrcurrentdata["INSTANCE_OF"] = strinstanceofid
                        print(f"{strwikidataid} '{strimdbid}' '{strname}' {strbirthdatesql}-{strdeathdatesql}")
                    # Accumulate aliases across rows for the current character
                    if 'aliasLabel.value' in row:
                        if row['aliasLabel.value']:
                            if not pd.isna(row['aliasLabel.value']):
                                stralias = row['aliasLabel.value']
                                if re.match(r'^[QPL]\d+$', stralias):
                                    stralias = ""
                                if stralias != "" and stralias not in arrcurrentaliases:
                                    arrcurrentaliases.append(stralias)
                # End of the loop for the current query so we flush the last character
                if strwikidataidprev != "":
                    if arrcurrentaliases:
                        arrcurrentdata["ALIASES"] = ", ".join(arrcurrentaliases)
                    strsqltablename = "T_WC_WIKIDATA_CHARACTER_V1"
                    strsqlupdatecondition = f"ID_WIKIDATA = '{strwikidataidprev}'"
                    cp.f_sqlupdatearray(strsqltablename,arrcurrentdata,strsqlupdatecondition,1)
                    arrcurrentdata = {}
                    arrcurrentaliases = []
                    strwikidataidprev = ""
            intencore = False
        except SPARQLExceptions.EndPointInternalError as e:
            print(f"Internal Server Error: {e}")
        except SPARQLExceptions.QueryBadFormed as e:
            # Permanent error: a malformed SPARQL query will never succeed on retry
            print(f"Badly Formed Query: {e}")
            intencore = False
        except SPARQLExceptions.EndPointNotFound as e:
            # Permanent error: a wrong endpoint URL will never succeed on retry
            print(f"Endpoint Not Found: {e}")
            intencore = False
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Honor server-suggested Retry-After when present; default 120s otherwise
                # (WDQS sometimes drops to 1 req/min during outages, so 60s is too tight)
                lngretryafter = 120
                strretryafter = ""
                try:
                    if e.headers is not None:
                        strretryafter = e.headers.get("Retry-After", "") or ""
                except Exception:
                    strretryafter = ""
                if strretryafter:
                    try:
                        lngretryafter = int(strretryafter)
                    except (ValueError, TypeError):
                        lngretryafter = 120
                print(f"HTTP 429 rate-limited by Wikidata. Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
            else:
                print(f"HTTP Error {e.code}: {e}")
                lngretryafter = 60
                print(f"Retrying after {lngretryafter} seconds.")
                time.sleep(lngretryafter)
        except Exception as e:
            print(f"An error occurred: {e}")
            lngretryafter = 60
            print(f"Retrying after {lngretryafter} seconds.")
            time.sleep(lngretryafter)

strprocessesexecutedprevious = cp.f_getservervariable("strsparqlaltcrawlermoviespersonsprocessesexecuted",0)
strprocessesexecuteddesc = "List of processes executed in the Wikidata SPARQL alternative crawler"
cp.f_setservervariable("strsparqlaltcrawlerprocessesexecutedprevious",strprocessesexecutedprevious,strprocessesexecuteddesc + " (previous execution)",0)
strprocessesexecuted = ""
cp.f_setservervariable("strsparqlaltcrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)

try:
    conn = cp.f_getconnection()
    with conn:
        with conn.cursor() as cursor:
            cursor3 = conn.cursor()
            # Start timing the script execution
            start_time = time.time()
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strsparqlaltcrawlerstartdatetime",strnow,"Date and time of the last start of the Wikidata SPARQL alternative crawler",0)
            strtotalruntimedesc = "Total runtime of the Wikidata SPARQL crawler for movies, series and persons"
            strtotalruntimeprevious = cp.f_getservervariable("strsparqlaltcrawlermoviespersonstotalruntime",0)
            cp.f_setservervariable("strsparqlaltcrawlertotalruntimeprevious",strtotalruntimeprevious,strtotalruntimedesc + " (previous execution)",0)
            strtotalruntime = "RUNNING"
            cp.f_setservervariable("strsparqlaltcrawlertotalruntime",strtotalruntime,strtotalruntimedesc,0)
            # Request Homer
            #f_sparqlpersonscrawl("Q6691",0)
            # Retrieving instance of values for persons (humans) used in Wikidata Sparql queries
            strsparqlpersoninstanceof = cp.f_getservervariable("strsparqlaltcrawlerpersoninstanceof",0)
            if strsparqlpersoninstanceof == "":
                strsparqlpersoninstanceof = "Q5"
                cp.f_setservervariable("strsparqlaltcrawlerpersoninstanceof",strsparqlpersoninstanceof,"Instances of values for persons (humans) used in Wikidata Sparql queries",0)
            # Retrieving instance of values for movies used in Wikidata Sparql queries
            strsparqlmovieinstanceof = cp.f_getservervariable("strsparqlaltcrawlermovieinstanceof",0)
            if strsparqlmovieinstanceof == "":
                strsparqlmovieinstanceof = "Q11424 Q202866 Q226730 Q24862 Q20650540 Q506240 Q17517379"
                cp.f_setservervariable("strsparqlaltcrawlermovieinstanceof",strsparqlmovieinstanceof,"Instances of values for movies used in Wikidata Sparql queries",0)
            # Retrieving instance of values for series used in Wikidata Sparql queries
            strsparqlserieinstanceof = cp.f_getservervariable("strsparqlaltcrawlerserieinstanceof",0)
            if strsparqlserieinstanceof == "":
                strsparqlserieinstanceof = "Q5398426 Q1259759 Q117467246 Q63952888 Q15416"
                cp.f_setservervariable("strsparqlaltcrawlerserieinstanceof",strsparqlserieinstanceof,"Instances of values for series used in Wikidata Sparql queries",0)
            # Retrieving instance of values for seasons used in Wikidata Sparql queries
            strsparqlseasoninstanceof = cp.f_getservervariable("strsparqlaltcrawlerseasoninstanceof",0)
            if strsparqlseasoninstanceof == "":
                strsparqlseasoninstanceof = "Q3464665"
                cp.f_setservervariable("strsparqlaltcrawlerseasoninstanceof",strsparqlseasoninstanceof,"Instances of values for seasons used in Wikidata Sparql queries",0)
            # Retrieving instance of values for episodes used in Wikidata Sparql queries
            strsparqlepisodeinstanceof = cp.f_getservervariable("strsparqlaltcrawlerepisodeinstanceof",0)
            if strsparqlepisodeinstanceof == "":
                strsparqlepisodeinstanceof = "Q21191270"
                cp.f_setservervariable("strsparqlaltcrawlerepisodeinstanceof",strsparqlepisodeinstanceof,"Instances of values for episodes used in Wikidata Sparql queries",0)
            # Retrieving instance of values for characters used in Wikidata Sparql queries
            strsparqlcharacterinstanceof = cp.f_getservervariable("strsparqlaltcrawlercharacterinstanceof",0)
            if strsparqlcharacterinstanceof == "":
                strsparqlcharacterinstanceof = "Q15632617 Q15773347 Q15773317 Q15711870 Q80447738 Q118247723 Q123126876"
                cp.f_setservervariable("strsparqlaltcrawlercharacterinstanceof",strsparqlcharacterinstanceof,"Instances of values for characters used in Wikidata Sparql queries",0)
            #arrwikidatascope = {101: 'movie', 102: 'person'}
            arrwikidatascope = {103: 'item to person', 104: 'item to movie', 106: 'item to serie', 109: 'item to season', 110: 'item to episode', 107: 'item to character', 102: 'person', 101: 'movie', 105: 'serie', 111: 'season', 112: 'episode', 113: 'serie to season', 114: 'serie to episode', 108: 'character', 115: 'work to character', 116: 'person to character'}
            #arrwikidatascope = {104: 'item to movie'}
            #arrwikidatascope = {103: 'item to person'}
            #arrwikidatascope = {105: 'serie'}
            #arrwikidatascope = {106: 'item to serie'}
            #if strnow.startswith("2026-05-24"):
            #    arrwikidatascope = {114: 'serie to episode', 108: 'character', 115: 'work to character', 116: 'person to character'}
            for intindex,strcontent in arrwikidatascope.items():
                strcurrentprocess = f"{intindex}: processing Wikidata " + strcontent + " data using SPARQL"
                strprocessesexecuted += str(intindex) + ", "
                cp.f_setservervariable("strsparqlaltcrawlerprocessesexecuted",strprocessesexecuted,strprocessesexecuteddesc,0)
                print(strcurrentprocess)
                if intindex == 101:
                    # Films data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 5
                    #lngyearbegin = 2025
                    lngyearend = 1875
                    #lngyearend = 2025
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlermoviescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlermoviescurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, movie process",0)
                        time.sleep(90)
                        # Retrieve all movies for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlmoviescrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 102:
                    # Humans data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year
                    lngyearend = 1000
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlerpersonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerpersonscurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, person process",0)
                        time.sleep(5)
                        # Retrieve all persons for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlpersonscrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 103:
                    # Items to persons data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrpersoninstanceof = [s.strip() for s in strsparqlpersoninstanceof.split() if s.strip()]
                    strpersoninstanceofsql = ", ".join([f"'{s}'" for s in arrpersoninstanceof])
                    if strpersoninstanceofsql == "":
                        strpersoninstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strpersoninstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        # Batch WDQS calls instead of one HTTP round-trip per id:
                        # a single VALUES ?item { wd:Q1 wd:Q2 ... } query returns many persons at once,
                        # cuts rate-limit pressure by ~lngbatchsize, and avoids the 1000s 429 back-off loop.
                        arrpersonidsall = [row3['ID_WIKIDATA'] for row3 in results]
                        lngbatchsize = 500
                        for lngi in range(0, len(arrpersonidsall), lngbatchsize):
                            arrbatch = arrpersonidsall[lngi:lngi + lngbatchsize]
                            strbatchids = " ".join(arrbatch)
                            strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                            cp.f_setservervariable("strsparqlaltcrawleritemstopersonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstopersonscurrentvalue",strbatchlabel,"Current Wikidata id batch in the Wikidata SPARQL alternative crawler, person process",0)
                            time.sleep(2)
                            # Retrieve all persons for this batch in a single SPARQL call
                            print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                            f_sparqlpersonscrawl(strbatchids,0)
                            # Drop the whole batch from T_WC_WIKIDATA_ITEM_V1 — ids with no SPARQL result
                            # are still considered processed (matches prior single-id behaviour)
                            strbatchsqllist = ",".join([f"'{x}'" for x in arrbatch])
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA IN (" + strbatchsqllist + ")"
                            cursor3.execute(strsqldelete)
                elif intindex == 104:
                    # Items to movies data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrmovieinstanceof = [s.strip() for s in strsparqlmovieinstanceof.split() if s.strip()]
                    strmovieinstanceofsql = ", ".join([f"'{s}'" for s in arrmovieinstanceof])
                    if strmovieinstanceofsql == "":
                        strmovieinstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strmovieinstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        # Batch WDQS calls instead of one HTTP round-trip per id:
                        # a single VALUES ?item { wd:Q1 wd:Q2 ... } query returns many movies at once,
                        # cuts rate-limit pressure by ~lngbatchsize, and avoids the 1000s 429 back-off loop.
                        arrmovieidsall = [row3['ID_WIKIDATA'] for row3 in results]
                        lngbatchsize = 500
                        for lngi in range(0, len(arrmovieidsall), lngbatchsize):
                            arrbatch = arrmovieidsall[lngi:lngi + lngbatchsize]
                            strbatchids = " ".join(arrbatch)
                            strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                            cp.f_setservervariable("strsparqlaltcrawleritemstomoviescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstomoviescurrentvalue",strbatchlabel,"Current Wikidata id batch in the Wikidata SPARQL alternative crawler, movie process",0)
                            time.sleep(2)
                            # Retrieve all movies for this batch in a single SPARQL call
                            print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                            f_sparqlmoviescrawl(strbatchids,0)
                            # Drop the whole batch from T_WC_WIKIDATA_ITEM_V1 — ids with no SPARQL result
                            # are still considered processed (matches prior single-id behaviour)
                            strbatchsqllist = ",".join([f"'{x}'" for x in arrbatch])
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA IN (" + strbatchsqllist + ")"
                            cursor3.execute(strsqldelete)
                elif intindex == 106:
                    # Items to series data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrserieinstanceof = [s.strip() for s in strsparqlserieinstanceof.split() if s.strip()]
                    strserieinstanceofsql = ", ".join([f"'{s}'" for s in arrserieinstanceof])
                    if strserieinstanceofsql == "":
                        strserieinstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strserieinstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_SERIE_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        # Batch WDQS calls instead of one HTTP round-trip per id:
                        # a single VALUES ?item { wd:Q1 wd:Q2 ... } query returns many series at once,
                        # cuts rate-limit pressure by ~lngbatchsize, and avoids the 1000s 429 back-off loop.
                        arrserieidsall = [row3['ID_WIKIDATA'] for row3 in results]
                        lngbatchsize = 500
                        for lngi in range(0, len(arrserieidsall), lngbatchsize):
                            arrbatch = arrserieidsall[lngi:lngi + lngbatchsize]
                            strbatchids = " ".join(arrbatch)
                            strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                            cp.f_setservervariable("strsparqlaltcrawleritemstoseriescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstoseriescurrentvalue",strbatchlabel,"Current Wikidata id batch in the Wikidata SPARQL alternative crawler, series process",0)
                            time.sleep(2)
                            # Retrieve all series for this batch in a single SPARQL call
                            print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                            f_sparqlseriescrawl(strbatchids,0)
                            # Drop the whole batch from T_WC_WIKIDATA_ITEM_V1 — ids with no SPARQL result
                            # are still considered processed (matches prior single-id behaviour)
                            strbatchsqllist = ",".join([f"'{x}'" for x in arrbatch])
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA IN (" + strbatchsqllist + ")"
                            cursor3.execute(strsqldelete)
                elif intindex == 109:
                    # Items to seasons data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrseasoninstanceof = [s.strip() for s in strsparqlseasoninstanceof.split() if s.strip()]
                    strseasoninstanceofsql = ", ".join([f"'{s}'" for s in arrseasoninstanceof])
                    if strseasoninstanceofsql == "":
                        strseasoninstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strseasoninstanceofsql + ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        arrseasonidsall = [row3['ID_WIKIDATA'] for row3 in results]
                        lngbatchsize = 500
                        for lngi in range(0, len(arrseasonidsall), lngbatchsize):
                            arrbatch = arrseasonidsall[lngi:lngi + lngbatchsize]
                            strbatchids = " ".join(arrbatch)
                            strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                            cp.f_setservervariable("strsparqlaltcrawleritemstoseasonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstoseasonscurrentvalue",strbatchlabel,"Current Wikidata id batch in the Wikidata SPARQL alternative crawler, season process",0)
                            time.sleep(2)
                            print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                            f_sparqlseasonscrawl(strbatchids,0)
                            strbatchsqllist = ",".join([f"'{x}'" for x in arrbatch])
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA IN (" + strbatchsqllist + ")"
                            cursor3.execute(strsqldelete)
                elif intindex == 110:
                    # Items to episodes data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrepisodeinstanceof = [s.strip() for s in strsparqlepisodeinstanceof.split() if s.strip()]
                    strepisodeinstanceofsql = ", ".join([f"'{s}'" for s in arrepisodeinstanceof])
                    if strepisodeinstanceofsql == "":
                        strepisodeinstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strepisodeinstanceofsql + ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        arrepisodeidsall = [row3['ID_WIKIDATA'] for row3 in results]
                        lngbatchsize = 500
                        for lngi in range(0, len(arrepisodeidsall), lngbatchsize):
                            arrbatch = arrepisodeidsall[lngi:lngi + lngbatchsize]
                            strbatchids = " ".join(arrbatch)
                            strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                            cp.f_setservervariable("strsparqlaltcrawleritemstoepisodescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstoepisodescurrentvalue",strbatchlabel,"Current Wikidata id batch in the Wikidata SPARQL alternative crawler, episode process",0)
                            time.sleep(2)
                            print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                            f_sparqlepisodescrawl(strbatchids,0)
                            strbatchsqllist = ",".join([f"'{x}'" for x in arrbatch])
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA IN (" + strbatchsqllist + ")"
                            cursor3.execute(strsqldelete)
                elif intindex == 107:
                    # Items to characters data download
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_ITEM_V1 "
                    arrcharacterinstanceof = [s.strip() for s in strsparqlcharacterinstanceof.split() if s.strip()]
                    strcharacterinstanceofsql = ", ".join([f"'{s}'" for s in arrcharacterinstanceof])
                    if strcharacterinstanceofsql == "":
                        strcharacterinstanceofsql = "'0'"
                    strsql += "WHERE INSTANCE_OF IN (" + strcharacterinstanceofsql + ") "
                    #strsql += "AND ID_WIKIDATA NOT IN ( "
                    #strsql += "SELECT ID_WIKIDATA FROM T_WC_WIKIDATA_CHARACTER_V1 "
                    #strsql += ") "
                    strsql += "ORDER BY ID_WIKIDATA "
                    # strsql += "LIMIT 1 "
                    if strsql != "":
                        print(strsql)
                        cursor3.execute(strsql)
                        lngrowcount = cursor3.rowcount
                        print(f"{lngrowcount} lines")
                        results = cursor3.fetchall()
                        # Batch WDQS calls instead of one HTTP round-trip per id:
                        # a single VALUES ?item { wd:Q1 wd:Q2 ... } query returns many characters at once,
                        # cuts rate-limit pressure by ~lngbatchsize, and avoids the 1000s 429 back-off loop.
                        arrcharacteridsall = [row3['ID_WIKIDATA'] for row3 in results]
                        lngbatchsize = 500
                        for lngi in range(0, len(arrcharacteridsall), lngbatchsize):
                            arrbatch = arrcharacteridsall[lngi:lngi + lngbatchsize]
                            strbatchids = " ".join(arrbatch)
                            strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                            cp.f_setservervariable("strsparqlaltcrawleritemstocharacterscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                            cp.f_setservervariable("strsparqlaltcrawleritemstocharacterscurrentvalue",strbatchlabel,"Current Wikidata id batch in the Wikidata SPARQL alternative crawler, character process",0)
                            time.sleep(2)
                            # Retrieve all characters for this batch in a single SPARQL call
                            print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                            f_sparqlcharactercrawl(strbatchids,0)
                            # Drop the whole batch from T_WC_WIKIDATA_ITEM_V1 — ids with no SPARQL result
                            # are still considered processed (matches prior single-id behaviour)
                            strbatchsqllist = ",".join([f"'{x}'" for x in arrbatch])
                            strsqldelete = "DELETE FROM T_WC_WIKIDATA_ITEM_V1 WHERE ID_WIKIDATA IN (" + strbatchsqllist + ")"
                            cursor3.execute(strsqldelete)
                elif intindex == 105:
                    # Series data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 4
                    #lngyearbegin = 1999
                    lngyearend = 1925
                    #lngyearend = 2025
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlerseriescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerseriescurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, serie process",0)
                        time.sleep(90)
                        # Retrieve all series for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlseriescrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 111:
                    # Seasons data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 4
                    lngyearend = 1925
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlerseasonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerseasonscurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, season process",0)
                        time.sleep(90)
                        # Retrieve all seasons for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlseasonscrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 112:
                    # Episodes data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 4
                    lngyearend = 1925
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlerepisodescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerepisodescurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, episode process",0)
                        time.sleep(90)
                        # Retrieve all episodes for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlepisodescrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 113:
                    # Serie to seasons data download:
                    # iterate every known series and ask Wikidata for its P179 backlinks (seasons).
                    # Fills gaps that the year-driven path (111) misses when a season has no P580.
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_SERIE_V1 "
                    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                    strsql += "ORDER BY ID_WIKIDATA "
                    print(strsql)
                    cursor3.execute(strsql)
                    lngrowcount = cursor3.rowcount
                    print(f"{lngrowcount} lines")
                    results = cursor3.fetchall()
                    arrserieidsall = [row3['ID_WIKIDATA'] for row3 in results]
                    # Smaller batch than items-to-X (500): each series can fan out to many seasons,
                    # and the P179 backlink join is heavier than a flat VALUES ?item lookup.
                    lngbatchsize = 100
                    for lngi in range(0, len(arrserieidsall), lngbatchsize):
                        arrbatch = arrserieidsall[lngi:lngi + lngbatchsize]
                        strbatchids = " ".join(arrbatch)
                        strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                        cp.f_setservervariable("strsparqlaltcrawlerseriestoseasonscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerseriestoseasonscurrentvalue",strbatchlabel,"Current Wikidata serie id batch in the Wikidata SPARQL alternative crawler, serie-to-season process",0)
                        time.sleep(5)
                        print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                        f_sparqlseasonscrawl("",0,strbatchids)
                elif intindex == 114:
                    # Serie to episodes data download:
                    # iterate every known series and ask Wikidata for its episodes via UNION of
                    #   episode --P4908--> season --P179--> series  and  episode --P179--> series.
                    # Fills gaps that the year-driven path (112) misses when an episode has no P577.
                    strsql = ""
                    strsql += "SELECT DISTINCT ID_WIKIDATA "
                    strsql += "FROM T_WC_WIKIDATA_SERIE_V1 "
                    strsql += "WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' "
                    strsql += "ORDER BY ID_WIKIDATA "
                    print(strsql)
                    cursor3.execute(strsql)
                    lngrowcount = cursor3.rowcount
                    print(f"{lngrowcount} lines")
                    results = cursor3.fetchall()
                    arrserieidsall = [row3['ID_WIKIDATA'] for row3 in results]
                    # Much smaller batch: an episode-heavy show (Simpsons, Doctor Who) can return
                    # hundreds of rows per series, and Wikidata has a 30s query timeout.
                    lngbatchsize = 25
                    for lngi in range(0, len(arrserieidsall), lngbatchsize):
                        arrbatch = arrserieidsall[lngi:lngi + lngbatchsize]
                        strbatchids = " ".join(arrbatch)
                        strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                        cp.f_setservervariable("strsparqlaltcrawlerseriestoepisodescurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerseriestoepisodescurrentvalue",strbatchlabel,"Current Wikidata serie id batch in the Wikidata SPARQL alternative crawler, serie-to-episode process",0)
                        time.sleep(10)
                        print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                        f_sparqlepisodescrawl("",0,strbatchids)
                elif intindex == 108:
                    # Characters data download
                    lngoffset = -1
                    lngyearbegin = datetime.now().year + 5
                    lngyearend = 1800
                    lngyearquery = lngyearbegin
                    intencore = True
                    while intencore:
                        cp.f_setservervariable("strsparqlaltcrawlercharacterscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlercharacterscurrentvalue",str(lngyearquery),"Current year in the Wikidata SPARQL alternative crawler, character process",0)
                        time.sleep(90)
                        # Retrieve all characters for a given year
                        print("lngyearquery = " + str(lngyearquery))
                        f_sparqlcharactercrawl("",lngyearquery)
                        if lngyearquery < lngyearend:
                            intencore = False
                        else:
                            lngyearquery += lngoffset
                elif intindex == 115:
                    # Work (movie + serie) to characters data download:
                    # iterate every known film and series and ask Wikidata for its P674 listed characters
                    # and P161/P453 cast-character qualifiers. Catches everything that the year-driven
                    # path (108) misses for lack of P577 / P4584 on the character itself.
                    arrworkidsall = []
                    strsql = "SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIDATA_SERIE_V1 WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' ORDER BY ID_WIKIDATA"
                    print(strsql)
                    cursor3.execute(strsql)
                    print(f"{cursor3.rowcount} serie lines")
                    arrworkidsall.extend([row3['ID_WIKIDATA'] for row3 in cursor3.fetchall()])
                    strsql = "SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIDATA_MOVIE_V1 WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' ORDER BY ID_WIKIDATA"
                    print(strsql)
                    cursor3.execute(strsql)
                    print(f"{cursor3.rowcount} movie lines")
                    arrworkidsall.extend([row3['ID_WIKIDATA'] for row3 in cursor3.fetchall()])
                    print(f"{len(arrworkidsall)} work ids total")
                    # Small batch: P161 cast statements can be hundreds per work on long-running shows.
                    lngbatchsize = 25
                    for lngi in range(0, len(arrworkidsall), lngbatchsize):
                        arrbatch = arrworkidsall[lngi:lngi + lngbatchsize]
                        strbatchids = " ".join(arrbatch)
                        strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                        cp.f_setservervariable("strsparqlaltcrawlerworktocharacterscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerworktocharacterscurrentvalue",strbatchlabel,"Current Wikidata work id batch in the Wikidata SPARQL alternative crawler, work-to-character process",0)
                        time.sleep(10)
                        print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                        f_sparqlcharactercrawl("",0,strbatchids,"")
                elif intindex == 116:
                    # Person to characters data download:
                    # iterate every known person and ask Wikidata for every (work, character) pair where
                    # the person was a cast member (P161) with a P453 character-role qualifier. Catches
                    # secondary / one-off roles that no work explicitly lists in P674.
                    # NOTE: T_WC_WIKIDATA_PERSON_V1 contains far more than actors, so most batches will
                    # return zero characters. The cost is still bounded by the per-batch SPARQL call.
                    strsql = "SELECT DISTINCT ID_WIKIDATA FROM T_WC_WIKIDATA_PERSON_V1 WHERE ID_WIKIDATA IS NOT NULL AND ID_WIKIDATA <> '' ORDER BY ID_WIKIDATA"
                    print(strsql)
                    cursor3.execute(strsql)
                    print(f"{cursor3.rowcount} person lines")
                    arrpersonidsall = [row3['ID_WIKIDATA'] for row3 in cursor3.fetchall()]
                    # Smaller batch than 115: a prolific actor's filmography can list hundreds of roles.
                    lngbatchsize = 15
                    for lngi in range(0, len(arrpersonidsall), lngbatchsize):
                        arrbatch = arrpersonidsall[lngi:lngi + lngbatchsize]
                        strbatchids = " ".join(arrbatch)
                        strbatchlabel = f"{arrbatch[0]}..{arrbatch[-1]} ({len(arrbatch)} ids)"
                        cp.f_setservervariable("strsparqlaltcrawlerpersontocharacterscurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL alternative crawler",0)
                        cp.f_setservervariable("strsparqlaltcrawlerpersontocharacterscurrentvalue",strbatchlabel,"Current Wikidata person id batch in the Wikidata SPARQL alternative crawler, person-to-character process",0)
                        time.sleep(10)
                        print(f"batch {lngi // lngbatchsize + 1}: {strbatchlabel}")
                        f_sparqlcharactercrawl("",0,"",strbatchids)
            strcurrentprocess = ""
            cp.f_setservervariable("strsparqlaltcrawlercurrentprocess",strcurrentprocess,"Current process in the Wikidata SPARQL crawler",0)
            strnow = datetime.now(cp.paris_tz).strftime("%Y-%m-%d %H:%M:%S")
            cp.f_setservervariable("strsparqlaltcrawlerenddatetime",strnow,"Date and time of the last end of the Wikidata SPARQL alternative crawler",0)
            # Calculate total runtime and convert to readable format
            end_time = time.time()
            strtotalruntime = int(end_time - start_time)  # Total runtime in seconds
            cp.f_setservervariable("strsparqlaltcrawlertotalruntimesecond",str(strtotalruntime),strtotalruntimedesc,0)
            readable_duration = cp.convert_seconds_to_duration(strtotalruntime)
            cp.f_setservervariable("strsparqlaltcrawlertotalruntime",readable_duration,strtotalruntimedesc,0)
            print(f"Total runtime: {strtotalruntime} seconds ({readable_duration})")
    
    print("Process completed")
except pymysql.MySQLError as e:
    print(f"❌ MySQL Error: {e}")
    conn = getattr(cp, "connectioncp", None)
    if conn is not None and getattr(conn, "open", False):
        conn.rollback()
