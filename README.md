## Rendu Neo4J

Luca Delanglade - Nicolas Lamy


Notre étude porte sur les livres prêtés en bibliothèques à Paris: <br>https://opendata.paris.fr/explore/dataset/tous-les-documents-des-bibliotheques-de-pret/information/ <br> Notre dataset contient les informations relatives au pret de chaque document disponible à l'emprunt et comptabilise le nombre de prets enregistrés sur l'année 2017. Nous avons souhaité le compléter avec des données accesibles sur l'API Googlebook.

Notre travail s'organise autour de 3 notebook: <br>
-notebook Data Preprocessing -- traitements éffectués en amont pour l'import sur Neo4J <br>
-notebook Data Visualisation -- analyser la distribution des données et restreindre l'étude <br>
-notebook Googlebook scrapping -- permet d'ajouter une colonne de description des livres au dataset <br>


Nous avons restreint notre étude à la catégorie litterature, qui comptabilise le plus grand nombre de prets à l'année : 200 000. Etant confronté à une limitation de 1000 requetes par jour via l'API Googlebook, nous avons importé les descriptions par batch de 1000.


```python
import pandas as pd
import numpy as np
pd.set_option('max_colwidth', 200)
data_neo4J=pd.read_csv("./data/data_litterature.csv").drop(['Unnamed: 0'],axis=1)
data_neo4J.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Titre</th>
      <th>sstitre</th>
      <th>Auteur</th>
      <th>Editeur</th>
      <th>Date</th>
      <th>prets</th>
      <th>Nombre de prêts par exemplaire</th>
      <th>exemplaires</th>
      <th>localisations</th>
      <th>Langue</th>
      <th>cat</th>
      <th>tauxemprunt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Chanson douce</td>
      <td></td>
      <td>Leïla Slimani</td>
      <td>Gallimard</td>
      <td>2016</td>
      <td>1169.0</td>
      <td>16.236111</td>
      <td>72.0</td>
      <td>51.0</td>
      <td>français</td>
      <td>LFRA Litterature francaise</td>
      <td>68.0</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Petit pays</td>
      <td></td>
      <td>Gaël Faye</td>
      <td>Bernard Grasset</td>
      <td>2016</td>
      <td>889.0</td>
      <td>14.338710</td>
      <td>62.0</td>
      <td>47.0</td>
      <td>français</td>
      <td>LFRA Litterature francaise</td>
      <td>60.0</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Sur les chemins noirs</td>
      <td></td>
      <td>Sylvain Tesson</td>
      <td>Gallimard</td>
      <td>2016</td>
      <td>856.0</td>
      <td>16.150943</td>
      <td>53.0</td>
      <td>49.0</td>
      <td>français</td>
      <td>LFRA Litterature francaise</td>
      <td>67.0</td>
    </tr>
    <tr>
      <td>3</td>
      <td>Arrête avec tes mensonges</td>
      <td></td>
      <td>Philippe Besson</td>
      <td>Julliard</td>
      <td>2017</td>
      <td>846.0</td>
      <td>16.588235</td>
      <td>51.0</td>
      <td>49.0</td>
      <td>français</td>
      <td>LFRA Litterature francaise</td>
      <td>69.0</td>
    </tr>
    <tr>
      <td>4</td>
      <td>Article 353 du code pénal</td>
      <td></td>
      <td>Tanguy Viel</td>
      <td>les Éditions de Minuit</td>
      <td>2017</td>
      <td>785.0</td>
      <td>14.811321</td>
      <td>53.0</td>
      <td>52.0</td>
      <td>français</td>
      <td>LFRA Litterature francaise</td>
      <td>62.0</td>
    </tr>
  </tbody>
</table>
</div>



## Import


```python
from neo4j import GraphDatabase

DB_URI = "bolt://localhost:7687"
DB_USER = "neo4j"
DB_PASSWORD = "0207"
driver = GraphDatabase.driver(DB_URI, auth=(DB_USER, DB_PASSWORD))
```


```python
#Contraintes
CONTRAINTES=[
#On identifie un ouvrage par son titre et son auteur
"CREATE CONSTRAINT ON (l:Livre) ASSERT (l.titre, l.auteur) IS NODE KEY;",
#On affecte une unique valeur au nom de l'auteur
"CREATE CONSTRAINT ON (a:Auteur) ASSERT a.nomauteur IS UNIQUE;",
"CREATE CONSTRAINT ON (e:Editeur) ASSERT e.nomediteur IS UNIQUE;",
"CREATE CONSTRAINT ON (c: Categorie) ASSERT c.nomcat IS UNIQUE;"]
for i in CONTRAINTES:
    with driver.session() as session:
        session.run(i)
```


```python
#Import des données principales. 

#On les metrics pour chaque ouvrage en aggrégeant les données provenant des différentes éditions de ces ouvrages
IMPORT1="""
USING PERIODIC COMMIT 500 

LOAD CSV WITH HEADERS FROM 'file:///data_litterature.csv' 
AS ligne with ligne where ligne.Titre is not null

   MERGE (l:Livre {titre: trim(ligne.Titre), auteur: trim(ligne.Auteur)}) 
   
   ON CREATE SET l.prets = ToInteger(ligne.prets) 
   ON MATCH SET l.prets = l.prets + ToInteger(ligne.prets) 
      
   ON CREATE SET l.exemplaires = ToInteger(ligne.exemplaires) 
   ON MATCH SET l.exemplaires = l.exemplaires + ToInteger(ligne.exemplaires)
    
   ON CREATE SET l.localisations = ToInteger(ligne.localisations) 
   ON MATCH SET l.localisations = l.localisations + ToInteger(ligne.localisations)

   ON CREATE SET l.soustitre = trim(ligne.sstitre)  
   ON CREATE SET l.langue = trim(ligne.Langue)
   
   SET l.editions =COALESCE(l.editions,[]) + 
   CASE WHEN NOT (trim(ligne.Editeur) +"_"+ trim(ligne.Date)) IN COALESCE(l.editions,[]) 
   THEN (trim(ligne.Editeur) +"_"+ trim(ligne.Date)) 
   END
   
   SET l.date=COALESCE(l.date,[]) + 
   CASE WHEN NOT ToInteger(ligne.Date) IN COALESCE(l.date,[]) 
   THEN ToInteger(ligne.Date) 
   END
 
   SET l.tauxemprunt =round(100-(24-l.prets/l.exemplaires)*100*0.0416666666)
   
   MERGE (auteur:Auteur { nomauteur: trim(ligne.Auteur) }) 
   
   MERGE (edi:Editeur { nomediteur: trim(ligne.Editeur) })
   
   MERGE (cat:Categorie { nomcat: trim(ligne.cat)}) 
   
   MERGE (l)-[: ECRIT]->(auteur)
   MERGE (l)-[: EDITE]->(edi) 
   MERGE (l)-[: APPARTIENT]->(cat) ;

"""

with driver.session() as session:
    session.run(IMPORT1)
```


```python
#Import des données de GoogleBook
IMPORT2="""

LOAD CSV WITH HEADERS FROM 'file:///Googlebook_db.csv' 
AS ligne with ligne where ligne.Titre is not null

   MERGE (l:Livre {titre: trim(ligne.Titre)}) SET l.summary = trim(ligne.Summary)
   
"""
with driver.session() as session:
    session.run(IMPORT2)
```

## REQUETES


```python
#Qui sont les auteurs les plus lus?

REQUETE1="""

MATCH (c:Categorie)--(l:Livre)--(a:Auteur) 
WITH a,c, collect(l.titre) as l ,collect(l.prets) as p, reduce(total=0, number in collect (l.prets) | total + number) as psum
RETURN a.nomauteur , l, p, psum, right(c.nomcat,length(c.nomcat)-4)
ORDER BY psum DESC
LIMIT 10

"""
with driver.session() as session:
    result=session.run(REQUETE1)
    df = pd.DataFrame(result, columns=["Auteur","Ouvrages","prets","Total prets","Catégorie"])

    
#Fonction pour trier les résultats par nombre de prets et faire un Top 5 des ouvrages pour chaque auteur
def func(x,y):
    r=pd.DataFrame(x,y).sort_values(by=0,ascending=False)
    return list(r.index), list(r.values.reshape(-1))

df["Top 5 Ouvrages"]=df.apply(lambda x:func(x[2],x[1])[0][:5],axis=1)
df["Top 5 prets"]=df.apply(lambda x:func(x[2],x[1])[1][:5],axis=1)
df=df.drop(["Ouvrages"],axis=1)[["Auteur","Total prets","Catégorie", "Top 5 Ouvrages", "Top 5 prets"]]
df.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Auteur</th>
      <th>Total prets</th>
      <th>Catégorie</th>
      <th>Top 5 Ouvrages</th>
      <th>Top 5 prets</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Anne Perry</td>
      <td>6089</td>
      <td>Litterature britannique</td>
      <td>[Un Noël à New York, Vengeance en eau froide, Un traître à Kensington Palace, Bryanston Mews, Meurtre en écho]</td>
      <td>[555, 401, 296, 221, 213]</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Harlan Coben</td>
      <td>5767</td>
      <td>Litterature nord-americaine</td>
      <td>[Intimidation, Six ans déjà, Tu me manques, À toute épreuve, Ne t'éloigne pas]</td>
      <td>[725, 432, 344, 301, 283]</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Michael Connelly</td>
      <td>5716</td>
      <td>Litterature nord-americaine</td>
      <td>[Mariachi Plaza, Jusqu'à l'impensable, Les dieux du verdict, Dans la ville en feu, Ceux qui tombent]</td>
      <td>[492, 469, 398, 307, 233]</td>
    </tr>
    <tr>
      <td>3</td>
      <td>Fred Vargas</td>
      <td>5283</td>
      <td>Litterature francaise</td>
      <td>[Temps glaciaires, Quand sort la recluse, Un lieu incertain, Sans feu ni lieu, L'homme à l'envers]</td>
      <td>[632, 595, 406, 364, 362]</td>
    </tr>
    <tr>
      <td>4</td>
      <td>Amélie Nothomb</td>
      <td>5272</td>
      <td>Litterature francaise</td>
      <td>[Frappe-toi le coeur, Riquet à la houppe, La nostalgie heureuse, Le crime du comte Neville, Pétronille]</td>
      <td>[633, 613, 343, 340, 332]</td>
    </tr>
    <tr>
      <td>5</td>
      <td>Mary Higgins Clark</td>
      <td>4973</td>
      <td>Litterature nord-americaine</td>
      <td>[Le piège de la Belle au bois dormant, La mariée était en blanc, Le temps des regrets, Noir comme la mer, La boîte à musique]</td>
      <td>[495, 444, 380, 351, 308]</td>
    </tr>
    <tr>
      <td>6</td>
      <td>Henning Mankell</td>
      <td>4499</td>
      <td>Litterature nordique</td>
      <td>[Les bottes suédoises, Une main encombrante, Les chaussures italiennes, Sable mouvant, Un paradis trompeur]</td>
      <td>[537, 399, 293, 202, 199]</td>
    </tr>
    <tr>
      <td>7</td>
      <td>Joyce Carol Oates</td>
      <td>4380</td>
      <td>Litterature nord-americaine</td>
      <td>[Valet de pique, Sacrifice, Daddy Love, Dahlia noir &amp; Rose blanche, Carthage]</td>
      <td>[456, 426, 215, 185, 166]</td>
    </tr>
    <tr>
      <td>8</td>
      <td>Stephen King</td>
      <td>4342</td>
      <td>Litterature nord-americaine</td>
      <td>[Fin de ronde, Le bazar des mauvais rêves, Carnets noirs, Mr Mercedes, Revival]</td>
      <td>[313, 293, 273, 248, 243]</td>
    </tr>
    <tr>
      <td>9</td>
      <td>James Patterson</td>
      <td>4338</td>
      <td>Litterature nord-americaine</td>
      <td>[Cours, Alex Cross !, Tue-moi si tu peux, 14e péché mortel, Invisible, Cross, coeur de cible]</td>
      <td>[229, 216, 216, 201, 174]</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Quels sont les livres américains les plus lus?
REQUETE2="""

MATCH (cat)--(l:Livre)--(a:Auteur)
MATCH (l:Livre)--(e:Editeur)
WHERE left(cat.nomcat,4)="LNAM"
WITH collect(e.nomediteur) as e, a, l
RETURN DISTINCT(l.titre),a.nomauteur,e, l.langue, l.prets
ORDER BY l.prets DESC
LIMIT 20

"""
with driver.session() as session:
    result=session.run(REQUETE2)
    df = pd.DataFrame(result, columns=["Titre","Auteur","Editeurs","Langue","Prets"])
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Titre</th>
      <th>Auteur</th>
      <th>Editeurs</th>
      <th>Langue</th>
      <th>Prets</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Intimidation</td>
      <td>Harlan Coben</td>
      <td>[Pocket, Feryane, Belfond]</td>
      <td>français</td>
      <td>725</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Divergente</td>
      <td>Veronica Roth</td>
      <td>[Pocket jeunesse, Éd. France loisirs, Nathan]</td>
      <td>français</td>
      <td>617</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Message sans réponse</td>
      <td>Patricia J. MacDonald</td>
      <td>[Editions Libra diffusio, Albin Michel]</td>
      <td>français</td>
      <td>597</td>
    </tr>
    <tr>
      <td>3</td>
      <td>La vengeance des mères</td>
      <td>Jim Fergus</td>
      <td>[Pocket, A vue d'oeil, Cherche midi]</td>
      <td>français</td>
      <td>539</td>
    </tr>
    <tr>
      <td>4</td>
      <td>The girls</td>
      <td>Emma Cline</td>
      <td>[Vintage, Random House, Chatto &amp; Windus, Quai Voltaire]</td>
      <td>français</td>
      <td>521</td>
    </tr>
    <tr>
      <td>5</td>
      <td>Le trône de fer</td>
      <td>George R. R. Martin</td>
      <td>[Pygmalion, J'ai lu]</td>
      <td>français</td>
      <td>506</td>
    </tr>
    <tr>
      <td>6</td>
      <td>Une avalanche de conséquences</td>
      <td>Elizabeth George</td>
      <td>[Pocket, Presses de la Cité]</td>
      <td>français</td>
      <td>495</td>
    </tr>
    <tr>
      <td>7</td>
      <td>Le piège de la Belle au bois dormant</td>
      <td>Mary Higgins Clark</td>
      <td>[Albin Michel]</td>
      <td>français</td>
      <td>495</td>
    </tr>
    <tr>
      <td>8</td>
      <td>Mariachi Plaza</td>
      <td>Michael Connelly</td>
      <td>[Le Livre de poche, Calmann-Lévy]</td>
      <td>français</td>
      <td>492</td>
    </tr>
    <tr>
      <td>9</td>
      <td>Le vieux saltimbanque</td>
      <td>Jim Harrison</td>
      <td>[J'ai lu, Flammarion]</td>
      <td>français</td>
      <td>482</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Quels sont les Livres les plus difficiles à emprunter? (Taux emprunt: 100%=2emprunts par mois par exemplaire)
REQUETE3="""

MATCH (cat)--(l:Livre)--(a:Auteur)
MATCH (l:Livre)--(e:Editeur)
WITH a,l,collect(distinct(e.nomediteur)) as e
RETURN l.titre, a.nomauteur, e, l.langue, l.tauxemprunt
ORDER BY l.tauxemprunt DESC
LIMIT 20

"""
pd.set_option('max_colwidth', 300)
with driver.session() as session:
    result=session.run(REQUETE3)
    df = pd.DataFrame(result, columns=["Titre","Auteur","Editeurs","Langue","Taux d'emprunt"])
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Titre</th>
      <th>Auteur</th>
      <th>Editeurs</th>
      <th>Langue</th>
      <th>Taux d'emprunt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Le café chat</td>
      <td>Melissa Daley</td>
      <td>[City éditions]</td>
      <td>français</td>
      <td>88.0</td>
    </tr>
    <tr>
      <td>1</td>
      <td>L'école du crime</td>
      <td>Christian Jacq</td>
      <td>[J éditions;XO]</td>
      <td>français</td>
      <td>79.0</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Sphinx</td>
      <td>Christian Jacq</td>
      <td>[XO éditions]</td>
      <td>français</td>
      <td>75.0</td>
    </tr>
    <tr>
      <td>3</td>
      <td>Vengeance haute couture</td>
      <td>Rosalie Ham</td>
      <td>[Mosaïc]</td>
      <td>français</td>
      <td>71.0</td>
    </tr>
    <tr>
      <td>4</td>
      <td>De pourpre et de soie</td>
      <td>Mary Chamberlain</td>
      <td>[Préludes]</td>
      <td>français</td>
      <td>71.0</td>
    </tr>
    <tr>
      <td>5</td>
      <td>Les disparues de Shanghai</td>
      <td>Peter May</td>
      <td>[Actes Sud;Leméac]</td>
      <td>français</td>
      <td>71.0</td>
    </tr>
    <tr>
      <td>6</td>
      <td>La promesse</td>
      <td>Cédric Cham</td>
      <td>[Éditions Fleur sauvage]</td>
      <td>français</td>
      <td>71.0</td>
    </tr>
    <tr>
      <td>7</td>
      <td>Zui man chang de na yi ye</td>
      <td>Jun Cai</td>
      <td>[Xian dai chu ban she]</td>
      <td>chinois</td>
      <td>67.0</td>
    </tr>
    <tr>
      <td>8</td>
      <td>La Fin de l'été</td>
      <td>Danielle Steel</td>
      <td>[Presses de la Cité]</td>
      <td>français</td>
      <td>67.0</td>
    </tr>
    <tr>
      <td>9</td>
      <td>Vingt-quatre heures pour convaincre une femme</td>
      <td>Philippe Lacoche</td>
      <td>[Écriture]</td>
      <td>français</td>
      <td>67.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Quels auteurs sont les mieux représentés en bibliothèque?

REQUETE4="""

MATCH (c:Categorie)--(l:Livre)--(a:Auteur)
WITH a, count(distinct(l.titre)) as rel, collect(l.titre) as l, collect(l.prets) as p, c, collect(l.exemplaires) as ex
RETURN a.nomauteur, l,p,rel,right(c.nomcat,length(c.nomcat)-4),ex
ORDER BY rel DESC
LIMIT 20

"""

with driver.session() as session:
    result=session.run(REQUETE4)
    df = pd.DataFrame(result,columns=["Auteur","Titres","prets","Nombre d'ouvrages","Catégorie","Exemplaires"])
    


df["Top 5 Ouvrages"]=df.apply(lambda x:func(x[2],x[1])[0][:5],axis=1)
df["Top 5 prets"]=df.apply(lambda x:func(x[2],x[1])[1][:5],axis=1)
df["Total prets"]=df.apply(lambda x:np.sum(x[2]),axis=1)
df["Total exemplaires"]=df.apply(lambda x:np.sum(x[5]),axis=1)
df=df.drop(["Titres","Exemplaires"],axis=1)[["Auteur","Nombre d'ouvrages","Catégorie", "Top 5 Ouvrages", "Top 5 prets", "Total prets","Total exemplaires"]]
df.head(20)


```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Auteur</th>
      <th>Nombre d'ouvrages</th>
      <th>Catégorie</th>
      <th>Top 5 Ouvrages</th>
      <th>Top 5 prets</th>
      <th>Total prets</th>
      <th>Total exemplaires</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Georges Simenon</td>
      <td>207</td>
      <td>Litterature francaise</td>
      <td>[Oeuvre romanesque, Tout Simenon, Tout Maigret, Les caves du Majestic, 0Romans]</td>
      <td>[622, 267, 186, 111, 101]</td>
      <td>3199</td>
      <td>977</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Agatha Christie</td>
      <td>199</td>
      <td>Litterature britannique</td>
      <td>[Agatha Christie, Dix petits nègres, Le meurtre de Roger Ackroyd, Le Noël d'Hercule Poirot, L'if et la rose]</td>
      <td>[759, 195, 141, 92, 89]</td>
      <td>4237</td>
      <td>1029</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Honoré de Balzac</td>
      <td>138</td>
      <td>Litterature francaise</td>
      <td>[La comédie humaine, Le père Goriot, Eugénie Grandet, La peau de chagrin, Le colonel Chabert]</td>
      <td>[531, 181, 170, 170, 140]</td>
      <td>3537</td>
      <td>1624</td>
    </tr>
    <tr>
      <td>3</td>
      <td>James Patterson</td>
      <td>135</td>
      <td>Litterature nord-americaine</td>
      <td>[Cours, Alex Cross !, Tue-moi si tu peux, 14e péché mortel, Invisible, Cross, coeur de cible]</td>
      <td>[229, 216, 216, 201, 174]</td>
      <td>4338</td>
      <td>1211</td>
    </tr>
    <tr>
      <td>4</td>
      <td>Danielle Steel</td>
      <td>125</td>
      <td>Litterature nord-americaine</td>
      <td>[Le fils prodigue, Une vie parfaite, Coup de foudre, Musique, Ambitions]</td>
      <td>[284, 207, 202, 181, 179]</td>
      <td>4168</td>
      <td>1069</td>
    </tr>
    <tr>
      <td>5</td>
      <td>Stephen King</td>
      <td>124</td>
      <td>Litterature nord-americaine</td>
      <td>[Fin de ronde, Le bazar des mauvais rêves, Carnets noirs, Mr Mercedes, Revival]</td>
      <td>[313, 293, 273, 248, 243]</td>
      <td>4342</td>
      <td>1406</td>
    </tr>
    <tr>
      <td>6</td>
      <td>Alexandre Dumas</td>
      <td>124</td>
      <td>Litterature francaise</td>
      <td>[Les trois mousquetaires, Le comte de Monte-Cristo, Le Comte de Monte-Cristo, La dame aux camélias, La reine Margot]</td>
      <td>[274, 224, 154, 143, 72]</td>
      <td>1968</td>
      <td>1042</td>
    </tr>
    <tr>
      <td>7</td>
      <td>Anne Perry</td>
      <td>118</td>
      <td>Litterature britannique</td>
      <td>[Un Noël à New York, Vengeance en eau froide, Un traître à Kensington Palace, Bryanston Mews, Meurtre en écho]</td>
      <td>[555, 401, 296, 221, 213]</td>
      <td>6089</td>
      <td>1310</td>
    </tr>
    <tr>
      <td>8</td>
      <td>Guy de Maupassant</td>
      <td>110</td>
      <td>Litterature francaise</td>
      <td>[Une vie, Bel-Ami, Contes et nouvelles, Le Horla, Pierre et Jean]</td>
      <td>[249, 249, 230, 227, 147]</td>
      <td>2806</td>
      <td>1001</td>
    </tr>
    <tr>
      <td>9</td>
      <td>Nora Roberts</td>
      <td>110</td>
      <td>Litterature nord-americaine</td>
      <td>[Des baisers sous la neige, Le menteur, L'auberge du mystère, Un coeur naufragé, Sasha]</td>
      <td>[201, 191, 183, 183, 173]</td>
      <td>2657</td>
      <td>771</td>
    </tr>
    <tr>
      <td>10</td>
      <td>Joyce Carol Oates</td>
      <td>105</td>
      <td>Litterature nord-americaine</td>
      <td>[Valet de pique, Sacrifice, Daddy Love, Dahlia noir &amp; Rose blanche, Carthage]</td>
      <td>[456, 426, 215, 185, 166]</td>
      <td>4380</td>
      <td>1490</td>
    </tr>
    <tr>
      <td>11</td>
      <td>Jules Verne</td>
      <td>100</td>
      <td>Litterature francaise</td>
      <td>[Voyage au centre de la terre, Le tour du monde en 80 jours, Vingt mille lieues sous les mers, L'île mystérieuse, 20 000 lieues sous les mers]</td>
      <td>[178, 157, 99, 86, 77]</td>
      <td>1616</td>
      <td>841</td>
    </tr>
    <tr>
      <td>12</td>
      <td>Terry Pratchett</td>
      <td>100</td>
      <td>Litterature britannique</td>
      <td>[La longue terre, La huitième couleur, Fond d'écran, De bons présages, La longue utopie]</td>
      <td>[96, 84, 72, 66, 65]</td>
      <td>1705</td>
      <td>901</td>
    </tr>
    <tr>
      <td>13</td>
      <td>Juliette Benzoni</td>
      <td>100</td>
      <td>Litterature francaise</td>
      <td>[Le vol du Sancy, Suite italienne, La petite peste et le chat botté, La fille du condamné, Les trois frères]</td>
      <td>[160, 139, 134, 109, 102]</td>
      <td>2071</td>
      <td>787</td>
    </tr>
    <tr>
      <td>14</td>
      <td>Victor Hugo</td>
      <td>99</td>
      <td>Litterature francaise</td>
      <td>[Les Misérables, Les misérables, Notre-Dame de Paris, L'homme qui rit, Roman]</td>
      <td>[361, 225, 217, 145, 111]</td>
      <td>2727</td>
      <td>1365</td>
    </tr>
    <tr>
      <td>15</td>
      <td>George Sand</td>
      <td>95</td>
      <td>Litterature francaise</td>
      <td>[La petite Fadette, François le Champi, Consuelo, Marianne, Oeuvres autobiographiques]</td>
      <td>[86, 76, 61, 52, 51]</td>
      <td>1066</td>
      <td>597</td>
    </tr>
    <tr>
      <td>16</td>
      <td>Serge Brussolo</td>
      <td>95</td>
      <td>Litterature francaise</td>
      <td>[L'oiseau des tempêtes, Tambours de guerre, Dortoir interdit, Cheval rouge, Le chat aux yeux jaunes]</td>
      <td>[152, 106, 68, 60, 58]</td>
      <td>1100</td>
      <td>483</td>
    </tr>
    <tr>
      <td>17</td>
      <td>Ruth Rendell</td>
      <td>94</td>
      <td>Litterature britannique</td>
      <td>[Les coins obscurs, Celle qui savait tout, Bon voisinage, Une vie si convenable, Un rossignol sans jardin]</td>
      <td>[471, 218, 198, 165, 164]</td>
      <td>2696</td>
      <td>899</td>
    </tr>
    <tr>
      <td>18</td>
      <td>Henry James</td>
      <td>94</td>
      <td>Litterature nord-americaine</td>
      <td>[0Nouvelles complètes, Daisy Miller, Le tour d'écrou, Ce que savait Maisie, Portrait de femme]</td>
      <td>[86, 85, 79, 70, 70]</td>
      <td>1251</td>
      <td>700</td>
    </tr>
    <tr>
      <td>19</td>
      <td>Michel Peyramaure</td>
      <td>94</td>
      <td>Litterature francaise</td>
      <td>[Couleurs Venise, La maison des tourbières, Le sabre de l'Empire, Trois cavaliers dans la forêt, Les rivales]</td>
      <td>[150, 90, 79, 65, 62]</td>
      <td>1071</td>
      <td>580</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Nous souhaitons voir quels sont les derniers livres édités par Gallimard qui sont disponibles à l'emprunt
REQUETE5="""

MATCH (e:Editeur{nomediteur:"Gallimard"})--(l:Livre)--(a:Auteur)
WHERE l.date IS NOT NULL
RETURN l.titre,a.nomauteur,reduce(m=0, t IN l.date | CASE WHEN m > t THEN m ELSE t END) as lastedition, l.tauxemprunt
ORDER BY lastedition DESC
LIMIT 20

"""

with driver.session() as session:
    result=session.run(REQUETE5)
    df = pd.DataFrame(result, columns=["Titre","Auteur","Date","Taux d'emprunt"])
 
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Titre</th>
      <th>Auteur</th>
      <th>Date</th>
      <th>Taux d'emprunt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0</td>
      <td>Le coeur content</td>
      <td>Nanoucha Van Moerkerkenland</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>1</td>
      <td>Casse-gueule</td>
      <td>Clarisse Gorokhoff</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>2</td>
      <td>Je voulais leur dire mon amour</td>
      <td>Jean-Noël Pancrazi</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>3</td>
      <td>Erri de Luca, entre Naples et la Bible</td>
      <td>Henri Godard</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>4</td>
      <td>Lynwood Miller</td>
      <td>Sandrine Roy</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>5</td>
      <td>Le syndrome de Garcin</td>
      <td>Jérôme Garcin</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>6</td>
      <td>Tous les chats sautent à leur façon</td>
      <td>Herta Müller</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>7</td>
      <td>Et moi, je vis toujours</td>
      <td>Jean d' Ormesson</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
    <tr>
      <td>8</td>
      <td>Les garçons de l'été</td>
      <td>Rebecca Lighieri</td>
      <td>2018</td>
      <td>17.0</td>
    </tr>
    <tr>
      <td>9</td>
      <td>Un si beau diplôme !</td>
      <td>Scholastique Mukasonga</td>
      <td>2018</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>


