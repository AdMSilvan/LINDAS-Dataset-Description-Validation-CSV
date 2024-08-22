#this version of the script additionally checks for the presence of alternative schema.org metadata to facilitate the mapping of existing metadata to DCAT


import requests
import csv
from dateutil.parser import parse
from datetime import datetime

response = requests.get("https://ld.admin.ch/query?format=csv&query=PREFIX%20vcard%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F2006%2Fvcard%2Fns%23%3E%0APREFIX%20sc%3A%20%3Chttp%3A%2F%2Fpurl.org%2Fscience%2Fowl%2Fsciencecommons%2F%3E%0APREFIX%20vo%3A%20%3Chttp%3A%2F%2Fpurl.obolibrary.org%2Fobo%2FVO_%3E%0APREFIX%20void%3A%20%3Chttp%3A%2F%2Frdfs.org%2Fns%2Fvoid%23%3E%0APREFIX%20dcat%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E%0APREFIX%20rdf%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0APREFIX%20rdfs%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0Aprefix%20dct%3A%20%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0Aprefix%20schema%3A%20%3Chttp%3A%2F%2Fschema.org%2F%3E%0ASELECT%20DISTINCT%20(%3Fsub%20AS%20%3FDataset)%0A(COUNT(DISTINCT%20%3Ftitl)%20AS%20%3Ftitle)%0A(COUNT(DISTINCT%20%3Fdesc)%20AS%20%3Fdescription)%0A(SAMPLE(COALESCE(%3Fpub%2C%20%22NONE%22))%20AS%20%3Fpublisher)%0A(SAMPLE(COALESCE(%3Fname%2C%20%22NONE%22))%20AS%20%3Fcontact_name)%0A(SAMPLE(COALESCE(%3Fmail%2C%20%22NONE%22))%20AS%20%3Fcontact_email)%0A(SAMPLE(COALESCE(%3Fid%2C%20%22NONE%22))%20AS%20%3Fidentifier)%0A(SAMPLE(COALESCE(%3Fdistr%2C%20%22NONE%22))%20AS%20%3Fdistribution)%0A%23(SAMPLE(COALESCE(%3Fdist_titl%2C%20%22NONE%22))%20AS%20%3Fdistribution_title)%0A%23(SAMPLE(COALESCE(%3Fdist_desc%2C%20%22NONE%22))%20AS%20%3Fdistribution_description)%0A%23(SAMPLE(COALESCE(%3Fdist_url%2C%20%22NONE%22))%20AS%20%3Fdistribution_url)%0A(SAMPLE(COALESCE(%3Fdate%2C%20%22NONE%22))%20AS%20%3Fpublication_date)%0A(SAMPLE(COALESCE(%3Fres%2C%20%22NONE%22))%20AS%20%3Fexample_resource)%0A(SAMPLE(COALESCE(%3Fend%2C%20%22NONE%22))%20AS%20%3FSPARQL_endpoint)%0A(SAMPLE(COALESCE(%3Furl%2C%20%22NONE%22))%20AS%20%3Faccess_URL)%0A(SAMPLE(COALESCE(%3Fmod_date%2C%20%22NONE%22))%20AS%20%3Fmodification_date)%0A(SAMPLE(COALESCE(%3Fcre%2C%20%22NONE%22))%20AS%20%3Fcreator)%0A(SAMPLE(COALESCE(%3Fcontr%2C%20%22NONE%22))%20AS%20%3Fcontributor)%0A(SAMPLE(COALESCE(%3Fpage%2C%20%22NONE%22))%20AS%20%3Flanding_page)%0A%0AWHERE%20%7B%0A%20%20%3Fsub%20a%20dcat%3ADataset%0A%20%20FILTER%20NOT%20EXISTS%20%7B%20%3Fsub%20schema%3Aexpires%20%3Fy%20%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Atitle%20%3Ftitl%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Adescription%20%3Fdesc%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Apublisher%20%3Fpub%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dcat%3AcontactPoint%20%3Fcont.%0A%20%20%20%20%3Fcont%20vcard%3AhasEmail%20%3Fmail%3B%20vcard%3Afn%20%3Fname%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Aidentifier%20%3Fid%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dcat%3Adistribution%20%3Fdistr%7D%0A%20%20%23OPTIONAL%20%7B%3Fdistr%20dct%3Atitle%20%3Fdist_titl%7D%0A%20%20%23OPTIONAL%20%7B%3Fdistr%20dct%3Adescription%20%3Fdist_desc%7D%0A%20%20%23OPTIONAL%20%7B%3Fdistr%20dcat%3AaccessURL%20%3Fdist_url%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Aissued%20%3Fdate%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20void%3AexampleResource%20%3Fres%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20void%3AsparqlEndpoint%20%3Fend%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dcat%3AaccessURL%20%3Furl%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Amodified%20%3Fmod_date%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dct%3Acreator%20%3Fcre%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3Acontributor%20%3Fcontr%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20dcat%3AlandingPage%20%3Fpage%7D%0A%7D%0AGROUP%20BY%20%3Fsub%0AORDER%20BY%20%3Fsub&endpoint=https%3A%2F%2Fld.admin.ch%2Fquery&requestMethod=POST&tabTitle=Query%203&headers=%7B%7D&contentTypeConstruct=application%2Fn-triples%2C*%2F*%3Bq%3D0.9&contentTypeSelect=application%2Fsparql-results%2Bjson%2C*%2F*%3Bq%3D0.9&outputFormat=table&outputSettings=%7B%22pageSize%22%3A1000%7D")
if response.status_code == 200:
    with open("query_result.csv", "wb") as f:
        f.write(response.content)
    print("DCAT Dataset metadata successfully downloaded")
else: 
    print("Failed to download DCAT dataset metadata")

response2 = requests.get("https://ld.admin.ch/query?format=csv&query=PREFIX%20sh%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fshacl%23%3E%0APREFIX%20vcard%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F2006%2Fvcard%2Fns%23%3E%0APREFIX%20sc%3A%20%3Chttp%3A%2F%2Fpurl.org%2Fscience%2Fowl%2Fsciencecommons%2F%3E%0APREFIX%20vo%3A%20%3Chttp%3A%2F%2Fpurl.obolibrary.org%2Fobo%2FVO_%3E%0APREFIX%20void%3A%20%3Chttp%3A%2F%2Frdfs.org%2Fns%2Fvoid%23%3E%0APREFIX%20dcat%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E%0APREFIX%20rdf%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23%3E%0APREFIX%20rdfs%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23%3E%0Aprefix%20dct%3A%20%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0Aprefix%20schema%3A%20%3Chttp%3A%2F%2Fschema.org%2F%3E%0ASELECT%20DISTINCT%20(%3Fsub%20AS%20%3FDataset)%0A(SAMPLE(COALESCE(%3Fpub%2C%20%22NONE%22))%20AS%20%3Fpublisher)%0A(SAMPLE(COALESCE(%3Fname%2C%20%22NONE%22))%20AS%20%3Fcontact_name)%0A(SAMPLE(COALESCE(%3Fmail%2C%20%22NONE%22))%20AS%20%3Fcontact_email)%0A(SAMPLE(COALESCE(%3Fid%2C%20%22NONE%22))%20AS%20%3Fidentifier)%0A(SAMPLE(COALESCE(%3Fdistr%2C%20%22NONE%22))%20AS%20%3Fdistribution)%0A(SAMPLE(COALESCE(%3Fdist_titl%2C%20%22NONE%22))%20AS%20%3Fdistribution_title)%0A(SAMPLE(COALESCE(%3Fdist_url%2C%20%22NONE%22))%20AS%20%3Fdistribution_url)%0A(SAMPLE(COALESCE(%3Fdate%2C%20%22NONE%22))%20AS%20%3Fpublication_date)%0A(SAMPLE(COALESCE(%3Fmod_date%2C%20%22NONE%22))%20AS%20%3Fmodification_date)%0A(SAMPLE(COALESCE(%3Fcre%2C%20%22NONE%22))%20AS%20%3Fcreator)%0A(SAMPLE(COALESCE(%3Fcontr%2C%20%22NONE%22))%20AS%20%3Fcontributor)%0A%0AWHERE%20%7B%0A%20%20%3Fsub%20a%20dcat%3ADataset%0A%20%20FILTER%20NOT%20EXISTS%20%7B%20%3Fsub%20schema%3Aexpires%20%3Fy%20%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3Apublisher%20%3Fpub%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3AcontactPoint%20%3Fcont.%0A%20%20%20%20%3Fcont%20schema%3Aemail%20%3Fmail%3B%0A%20%20%09schema%3Aname%20%3Fname%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3AworkExample%20%3Fdistr.%0A%20%20%3Fdistr%20schema%3AencodingFormat%20%22application%2Fsparql-query%22%3B%0A%20%20%20%20%20%20%20%20%20schema%3Aname%20%3Fdist_titl%3B%0A%20%20%20%20%20%20%20%20%20schema%3Aurl%20%3Fdist_url%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3AdatePublished%20%3Fdate%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3AdateModified%20%3Fmod_date%7D%0A%20%20OPTIONAL%20%7B%3Fsub%20schema%3Acreator%20%3Fcre%7D%0A%7D%0AGROUP%20BY%20%3Fsub%0AORDER%20BY%20%3Fsub&endpoint=https%3A%2F%2Fld.admin.ch%2Fquery&requestMethod=POST&tabTitle=Query%203&headers=%7B%7D&contentTypeConstruct=application%2Fn-triples%2C*%2F*%3Bq%3D0.9&contentTypeSelect=application%2Fsparql-results%2Bjson%2C*%2F*%3Bq%3D0.9&outputFormat=table")
if response2.status_code == 200:
    with open("schema_query_result.csv", "wb") as f:
        f.write(response2.content)
    print("Schema.org Dataset metadata successfully downloaded")
else: 
    print("Failed to download schema.org dataset metadata")



date_format = "%Y-%m-%d"

with open('query_result.csv', newline='') as i, open('Dataset_Description_Validation.csv', "w", newline='') as o, open('schema_query_result.csv', newline='') as s:
    reader = csv.DictReader(i)
    reader_s = csv.DictReader(s)
    writer = csv.writer(o)
    writer.writerow(["Conformance", "Dataset","title","description","publisher","contact","identifier","distribution","publication_date","example_resource","SPARQL_endpoint","access_URL","modification_date","creator","contributor","landing_page"])
    for row, row_s in zip(reader, reader_s):
        dataset_URI = row['Dataset']
        if row_s['Dataset'] == dataset_URI: pass
        else:
            print("schema.org dataset description not synched")
        #1 dataset
        row_output= [dataset_URI]
        #2 title
        title = row['title']
        if title == "0":
            row_output.append("No dct:title")
        else:
            row_output.append(f"conforms, {title} languages")

        #3 description
        description = row['description']
        if description == "0":
            row_output.append("No dct:description")
        else:
            row_output.append(f"conforms, {description} languages")

        #4 publisher
        publisher = row['publisher']
        if publisher == "NONE":
            if row_s['publisher'] != "NONE":
                row_output.append("No dct:publisher, but schema:publisher is provided")
            else:
                row_output.append("No dct:publisher")
        else:
            row_output.append("conforms")

        #5 contact
        contact_name = row['contact_name']
        contact_email = row['contact_email']
        if contact_name == "NONE" and contact_email == "NONE":
            if row_s['contact_name'] != "NONE" and row_s['contact_email'] != "NONE":
                row_output.append("Dcat:contactPoint has no vcard:fn (name) or vcard:hasEmail, but schema:email and schema:name are provided")
            else:
                row_output.append("Dcat:contactPoint has no vcard:fn (name) or vcard:hasEmail")
        elif contact_name == "NONE":
            if row_s['contact_name'] != "NONE":
                row_output.append("Dcat:contactPoint has no vcard:fn (name), but schema:name is provided")
            else:
                row_output.append("Dcat:contactPoint has no vcard:fn (name)")
        elif contact_email == "NONE":
            if row_s['contact_email'] != "NONE":
                row_output.append("Dcat:contactPoint has no vcard:hasEmail, but schema:email is provided")
            else:
                row_output.append("Dcat:contactPoint has no vcard:hasEmail")
        else:
            if "@" in contact_email:
                row_output.append("conforms")
            else:
                row_output.append("vcard:hasEmail not formatted correctly")  

        #6 identifier
        identifier = row['identifier']
        if identifier == "NONE":
            row_output.append("No dct:identifier")
        else:
            if "@" in identifier:
                row_output.append("conforms")
            else:
                row_output.append("dct:identifier not formatted correctly")            
        
        #7 distribution
        distribution = row['distribution']
        if distribution == "NONE":
            if row_s['distribution'] != "NONE":
                row_output.append("No dcat:distribution, but schema:workExample of a SPARQL Endpoint with graph preselection is provided")
            else:
                row_output.append("No dcat:distribution")
        else:
            row_output.append("Has a dcat:distribution")
            #could add SPARQLwrapper query here to check if distribution has necessary properties

        #8 publication_date
        publication_date = row['publication_date']
        if publication_date == "NONE":
            if row_s['publication_date'] != "NONE":
                try: 
                    parse(str(row_s['publication_date']))
                    row_output.append("No dct:issued (publication date), but schema:publicationDate is provided")
                except(ValueError):
                    row_output.append("No dct:issued (publication date)")
            else:
                row_output.append("No dct:issued (publication date)")
        else:
            try: 
                parse(str(publication_date))
                row_output.append("conforms")
            except(ValueError):
                row_output.append("dct:issued date is not formatted correctly")
        
        #9 example_resource
        example_resource = row['example_resource']
        if example_resource == "NONE":
            row_output.append("No void:exampleResource")
        elif dataset_URI.split("/")[2] in example_resource:
            row_output.append("conforms")
        else:
            row_output.append("void:exampleResource is not a URI in the dataset")

        #10 SPARQL_endpoint
        SPARQL_endpoint = row['SPARQL_endpoint']
        if SPARQL_endpoint == "NONE":
            row_output.append("No void:sparqlEndpoint")
        elif dataset_URI.split("/")[2] in SPARQL_endpoint and "query" in SPARQL_endpoint:
            row_output.append("conforms")
        else:
            row_output.append("void:sparqlEndpoint URL not formatted correctly")

        #11 access_URL
        access_URL = row['access_URL']
        if access_URL == "NONE":
            row_output.append("No dcat:access_URL")
        elif dataset_URI.split("/")[2] in access_URL and "sparql" in access_URL:
            row_output.append("conforms")
        else:
            row_output.append("dcat:access_URL not formatted correctly")
        
        #12 modification_date
        modification_date = row['modification_date']
        if modification_date == "NONE":
            if row_s['modification_date'] != "NONE":
                try: 
                    parse(str(row_s['modification_date']))
                    row_output.append("No dct:modified (modification date), but schema:modificationDate is provided")
                except(ValueError):
                    row_output.append("No dct:modified (modification date)")
            else:
                row_output.append("No dct:modified (modification date)")
        else:
            try: 
                parse(str(modification_date))
                row_output.append("conforms")
            except(ValueError):
                row_output.append("dct:modified date is not formatted correctly")
        
        #13 creator
        creator = row['creator']
        if creator == "NONE":
            if row_s['creator'] != "NONE" and "https://register.ld.admin.ch/opendataswiss/org/" in row_s['creator']:
                row_output.append("Optional: no dct:creator, but schema:creator is provided")
            else:
                row_output.append("Optional: no dct:creator")
        elif "https://register.ld.admin.ch/opendataswiss/org/" not in creator:
            row_output.append("dct:creator is not a opendataswiss organization URI")
        else:
            row_output.append("conforms")
        
        #14 contributor
        contributor = row['contributor']
        if contributor == "NONE":
            row_output.append("Optional: no dct:contributor")
        elif "https://register.ld.admin.ch/zefix/" in contributor:
            row_output.append("conforms")
        elif "https://register.ld.admin.ch/opendataswiss/org/" in contributor:
            row_output.append("conforms")
        else:
            row_output.append(f"dct:contributor not in Zefix or an opendataswiss organization: {contributor}")

        #15 landing page
        landing_page = row['landing_page']
        if landing_page == "NONE":
            row_output.append("Optional: no dcat:landingPage")
        elif "https://" not in landing_page and "http://" not in landing_page:
            row_output.append("dcat:landingPage URL not formatted correctly")
        else:
            row_output.append("conforms")

        #0 calculate overall Conformance
        missing = []
        for data in row_output[:-4]:
            if "conforms" not in data: missing.append(data)
            else: pass
        if len(missing) == 0:
            row_output.insert(0, "fully conformant")
        else:
            row_output.insert(0, f"({11-len(missing)}/11)")

        #write row
        if len(row_output) == 16:
            writer.writerow(row_output)
        elif len(row_output) > 16:
            print("row output too long")
            print(row)
            print(row_output)
            break
        else:
            print("row output too short \n query output:")
            print(row)
            print("conformance check output:")
            print(row_output)
            break
print("Dataset description validation successfully completed. \nResults saved to Dataset_Description_Validation.csv \nCompare to query_result.csv to check divergence.")
