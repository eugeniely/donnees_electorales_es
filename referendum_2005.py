import commun
import pandas as pd

input_file = "/Users/eugenie.ly/Documents/La REM/données/données électorales/référendum/2005/bureau de vote/référendum2005_bdv_DataGouv__RF05_BVot.csv"
output_file = '/Users/eugenie.ly/Documents/La REM/données/données électorales/référendum/2005/bureau de vote/référendum2005_bdv_pour_elasticsearch.csv'
separator = ';'

nom_election = "Référendum 2005"
date_election = '2005-05-29'

res_2005 = pd.read_csv(input_file, sep=';',
                       dtype={'N tour':str, 'Code région':str, 'Code département':str, 'Code arrondissement':str,
                              'Code circonscription législative':str, 'Code canton':str, 'Code de la commune':str,
                              'N° de bureau de vote':str, 'Inscrits':str, 'Votants':str, 'Abstentions':str,
                              'Exprimés':str, 'Réponse':str, 'Nombre de voix':str})
res_2005.drop(columns=['Code région', 'N tour', 'Code arrondissement', 'Inscrits de référence de la commune', 'Code canton', 'Unnamed: 16'], inplace=True)
res_2005.rename(columns={'Code département':'code_insee_departement_intermédiaire',
                         'Code circonscription législative':'code_circonscription', 'Code de la commune':'code_insee_commune_intermédiaire',
                         'Nom de la commune':'nom_commune_bdv', 'N° de bureau de vote':'code_bdv_intermédiaire',
                         'Inscrits':'nb_inscrits', 'Votants':'nb_votants', 'Abstentions':'nb_abstentions',
                         'Exprimés':'nb_exprimes', 'Réponse':'nom_complet_candidat', 'Nombre de voix':'nb_voix'}, inplace=True)

res_2005['code_insee_departement'] = res_2005['code_insee_departement_intermédiaire'].apply(lambda x: commun.change_code_dep(commun.padding(x, 2)))
res_2005['code_insee_commune_intermédiaire'] = res_2005['code_insee_departement'] + res_2005['code_insee_commune_intermédiaire'].apply(lambda x: commun.padding(x,3))
res_2005['code_insee_commune'] = res_2005['code_insee_commune_intermédiaire'].apply(lambda x: commun.code_insee_6_car(x))
res_2005['code_bdv'] = res_2005['code_insee_commune'].apply(lambda x: commun.padding(x,5)) + '_' + res_2005['code_bdv_intermédiaire'].apply(lambda x: commun.change_Caen_bdv(commun.padding(x,4)))
res_2005.drop(columns=['code_insee_commune_intermédiaire', 'code_bdv_intermédiaire', 'code_insee_departement_intermédiaire'], inplace=True)

res_2005['nom_candidat'] = res_2005['nom_complet_candidat']
res_2005['prenom_candidat'] = res_2005['nom_complet_candidat']

res_2005['abstentions_sur_inscrits_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_abstentions', 'nb_inscrits'), axis=1)
res_2005['abstentions_sur_votants_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_abstentions', 'nb_votants'), axis=1)
res_2005['exprimes_sur_inscrits_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_exprimes', 'nb_inscrits'), axis=1)
res_2005['exprimes_sur_votants_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_exprimes', 'nb_votants'), axis=1)
res_2005['votants_sur_inscrits_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_votants', 'nb_inscrits'), axis=1)
res_2005['voix_sur_inscrits_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_voix', 'nb_inscrits'), axis=1)
res_2005['voix_sur_exprimes_pc'] = res_2005.apply(lambda x: commun.pc(x, 'nb_voix', 'nb_exprimes'), axis=1)

col = ['nom_circonscription',
'code_bdv',
'nom_bdv',
'adresse_bdv',
'code_postal',
'ville_adresse_bdv',
'latitude_longitude_bdv']

commun.check_format(res_2005)
res1 = commun.traitement_election(res_2005, nom_election, date_election)
res = commun.jointure(res1, col)
res.to_csv(output_file, index=False)