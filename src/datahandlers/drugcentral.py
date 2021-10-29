from src.prefixes import DRUGCENTRAL
import psycopg2

def pull_drugcentral(structfile, labelfile, xreffile):
    #DrugCentral is only available as a postgres db, but fortunately they run a public instance.
    conn = psycopg2.connect("host=unmtid-dbs.net dbname=drugcentral user=drugman port=5433 password=dosage")
    cur = conn.cursor()
    cur.execute("SELECT * FROM identifier;")
    x = cur.fetchall()
    #Pull xrefs.  Output format is "1396790	C1172734	UMLSCUI	4970	None"
    # meaning UMLSCUI:C1172734 == DrugBank:4970
    with open(xreffile,'w') as outf:
        for xi in x:
            xs = [str(y) for y in xi]
            outf.write('\t'.join(xs))
            outf.write('\n')
    cur.execute("SELECT id, smiles, name FROM structures;")
    x = cur.fetchall()
    with open(labelfile, 'w') as outf, open(structfile, 'w') as sf:
        for xi in x:
            outf.write(f'{DRUGCENTRAL}:{xi[0]}\t{xi[2]}\n')
            sf.write(f'{DRUGCENTRAL}:{xi[0]}\t{xi[1]}\n')
