import pandas as pd
import geopandas as gpd
import json


def padding(x, length=5):
    if type(x) == str:
        if len(x) >= length:
            if x[0] == '-':
                return '0' + x[1:]
            return x
        if len(x) >= length - 1:
            return '0' + str(x)
        if len(x) >= length - 2:
            return '00' + str(x)
        if len(x) < length:
            out = x
            while len(out) != length:
                out = '0' + out
            return out
    if type(x) == int:
        if x >= 10 ** (length - 1):
            return str(x)
        if x >= 10 ** (length - 2):
            return '0' + str(x)
    if type(x) == float and (x is not None):
        if x >= 10 ** (length - 1):
            return str(int(x))
        if x >= 10 ** (length - 2):
            return '0' + str(int(x))
    return ''


def merge_epci(df):
    filepath = '/Users/eugenie.ly/Documents/La REM/données/arborescences/epci/epcicom2018_4.xls'
    filesheet = 'Epcicom2018'
    epci = pd.read_excel(filepath, sheet_name=filesheet, header=0,
                         converters={'dept': str, 'siren': str, 'raison_sociale': str, 'nature_juridique': str,
                                     'mode_financ': str, 'nb_membres': str, 'total_pop_tot': str, 'total_pop_mun': str,
                                     'dep_com': str, 'insee': str, 'siren_membre': str, 'nom_membre': str,
                                     'ptot_2018': str,
                                     'pmun_2018': str})
    columns = ['siren', 'raison_sociale', 'nature_juridique',
               'mode_financ', 'nb_membres', 'total_pop_tot', 'insee']
    for c in columns:
        epci[c] = epci[c].str.title()
    renamed_columns = {'siren': 'code_siren_epci',
                       'raison_sociale': 'nom_epci',
                       'nature_juridique': 'nature_juridique_epci',
                       'mode_financ': 'mode_financement_epci',
                       'nb_membres': 'nb_communes_epci',
                       'total_pop_tot': 'population_epci',
                       'insee': 'code_insee_commune'}
    epci = epci[columns].rename(columns=renamed_columns)
    return pd.merge(df, epci, on='code_insee_commune', how='left')


def change_code_bdv(x, old_code_insee_commune, left_new_code_insee):
    longueur_radical_gauche_code_insee = len(left_new_code_insee)
    if x[:5] == old_code_insee_commune:
        return left_new_code_insee+x[3+longueur_radical_gauche_code_insee:8]+x[5:]
    return x


def change_nom_commune(x, old_code_insee_commune):
    if x['code_insee_commune'] == old_code_insee_commune:
        if x['code_bdv'][6] == '0':
            if x['code_bdv'][7] == '1':
                return x['nom_commune_bdv'] + ' ' + x['code_bdv'][7] + 'Er Arrondissement'
            return x['nom_commune_bdv'] + ' ' + x['code_bdv'][7] + 'Ème Arrondissement'
        return x['nom_commune_bdv'] + ' ' + x['code_bdv'][6:8] + 'Ème Arrondissement'
    return x['nom_commune_bdv']


def change_code_insee(x):
    return x['code_bdv'][:5]


def paris_id_bdv_to_code_bdv(x, left_new_code_insee):
    l = x.split('-')
    len_right_new_code_insee = 5 - len(left_new_code_insee)
    return left_new_code_insee + padding(l[0], len_right_new_code_insee) + '_' \
           + padding(l[0], len_right_new_code_insee)\
           + padding(l[1], len_right_new_code_insee)


def traitement_paris(df):
    filepath = "/Users/eugenie.ly/Documents/La REM/données/fonds de carte/bureaux de vote/" \
               "Paris_2017T1bureaux-de-votes.csv"
    paris_geopoint = pd.read_csv(filepath, sep=';',
                                 dtype={'ObjectID': str, 'Numéro du bureau de vote': str,
                                        'Libellé du bureau de vote': str, 'Adresse': str, 'Code Postal': str,
                                        'Identifiant du bureau de vote': str, 'geo_shape': str, 'geo_point_2d': str})
    res_paris = df[df['code_insee_commune'] == '75056']
    res = df[df['code_insee_commune'] != '75056']
    res_paris['code_bdv'] = res_paris['code_bdv'].apply(lambda x: change_code_bdv(x, '75056', '751'))
    res_paris['nom_commune_bdv'] = res_paris.apply(lambda x: change_nom_commune(x, '75056'), axis=1)
    res_paris['code_insee_commune'] = res_paris.apply(lambda x: change_code_insee(x), axis=1)
    paris_geopoint['code_bdv'] = paris_geopoint['Identifiant du bureau de vote'].apply(
        lambda x: paris_id_bdv_to_code_bdv(x, '751'))
    res_paris_jointure = pd.merge(res_paris, paris_geopoint, on='code_bdv', how='left')
    res_paris_jointure['adresse_bdv'] = res_paris_jointure['Adresse'].str.title()
    res_paris_jointure['code_postal'] = res_paris_jointure['Code Postal']
    res_paris_jointure['nom_bdv'] = res_paris_jointure['Libellé du bureau de vote'].str.title()
    res_paris_jointure['latitude_longitude_bdv'] = res_paris_jointure['geo_point_2d']
    res_paris_jointure['ville_adresse_bdv'] = 'PARIS'
    return res.append(res_paris_jointure[res.columns.tolist()], ignore_index=True)


def lyon_id_bdv_to_code_bdv(x, left_new_code_insee):
    return left_new_code_insee + x['arrondisse'] + '_0' + x['num_bureau']


def lyon_code_postal(x, left_code_postal):
    longueur_radical_gauche_code_postal = len(left_code_postal)
    return left_code_postal + x[3 + longueur_radical_gauche_code_postal:8]


def lyon_adresse_bdv(x):
    if x['adresse'] is None:
        if x['voie_lieu_'] is None:
            return x['code_postal'] + ' LYON'
        return x['voie_lieu_'] + ' ' + x['code_postal'] + ' LYON'
    return x['adresse']


def lyon_latitude_longitude(x, order=0):
    if type(x) == float:
        return ''
    if order == 0:
        return str(x[0]) + ', ' + str(x[1])
    if order == 1:
        return str(x[1]) + ', ' + str(x[0])


def traitement_lyon(df):
    filepath = '/Users/eugenie.ly/Documents/La REM/données/fonds de carte/bureaux de vote/lyon_vie_citoyenne_bureau_de_vote.geojson'
    lyon_geopoint = pd.DataFrame()

    with open(filepath) as f_in:
        line = f_in.readline()
        L_bdv = line.split('},{"type":"Feature",')
        # element_premier = L_bdv[0]
        # element_dernier = L_bdv[-1]
        L_bdv[0] = L_bdv[0].split('{"type": "FeatureCollection", "features": [{"type":"Feature",')[1]
        L_bdv[-1] = L_bdv[-1][:-3]
        L_bdv = ['{' + bdv + '}' for bdv in L_bdv]
        for bdv_str in L_bdv:
            bdv_str_to_dict = json.loads(bdv_str)
            bdv_dict = bdv_str_to_dict['geometry'].update(bdv_str_to_dict['properties'])
            lyon_geopoint = lyon_geopoint.append(bdv_str_to_dict['geometry'], ignore_index=True)

    f_in.close()
    res_lyon = df[df['code_insee_commune'] == '69123']
    res = df[df['code_insee_commune'] != '69123']
    res_lyon['code_bdv'] = res_lyon['code_bdv'].apply(lambda x: change_code_bdv(x, '69123', '6938'))
    res_lyon['nom_commune_bdv'] = res_lyon.apply(lambda x: change_nom_commune(x, '69123'), axis=1)
    res_lyon['code_insee_commune'] = res_lyon.apply(lambda x: change_code_insee(x), axis=1)
    lyon_geopoint['code_bdv'] = lyon_geopoint.apply(lambda x: lyon_id_bdv_to_code_bdv(x, '6938'), axis=1)
    res_lyon_jointure = pd.merge(res_lyon, lyon_geopoint, on='code_bdv', how='left')
    res_lyon_jointure['code_postal'] = res_lyon_jointure['code_bdv'].apply(
        lambda x: lyon_code_postal(x, '6900')).str.title()
    res_lyon_jointure['adresse_bdv'] = res_lyon_jointure.apply(lambda x: lyon_adresse_bdv(x), axis=1).str.title()
    res_lyon_jointure['nom_bdv'] = res_lyon_jointure['lieu_de_vo'].str.title()
    res_lyon_jointure['latitude_longitude_bdv'] = res_lyon_jointure['coordinates'].apply(
        lambda x: lyon_latitude_longitude(x, 1))
    res_lyon_jointure['ville_adresse_bdv'] = 'LYON'
    return res.append(res_lyon_jointure[df.columns.tolist()], ignore_index=True)


def marseille_id_bdv_to_code_bdv(x, left_new_code_insee):
    if len(x) == 3:
        code_commune_intermediaire = x[0]
        code_bdv_intermediaire = x[1:]
    elif len(x) == 4:
        code_commune_intermediaire = x[:2]
        code_bdv_intermediaire = x[2:]
    else:
        code_commune_intermediaire = ''
        code_bdv_intermediaire = ''
    len_right_new_code_insee = 5 - len(left_new_code_insee)
    return left_new_code_insee + padding(code_commune_intermediaire, len_right_new_code_insee) + '_' + padding(code_commune_intermediaire, len_right_new_code_insee) + padding(code_bdv_intermediaire, len_right_new_code_insee)


def marseille_nom_bdv(x):
    if type(x) == float:
        return ''
    if x[:6] == 'E MAT ':
        return 'école maternelle ' + x[6:]
    if x[:7] == 'E ELEM ':
        return 'école maternelle ' + x[7:]
    if x[:8] == 'GR SCOL ':
        return 'école maternelle ' + x[8:]
    return x


def marseille_adresse_bdv(x):
    if type(x) == str:
        return x['Adr1'] + ' Marseille 130' + x['code_bdv'][-4:-2]
    if type(x) == float:
        return ''


def traitement_marseille(df):
    filepath = "/Users/eugenie.ly/Documents/La REM/données/fonds de carte/bureaux de vote/marseille_bdv.geojson"
    marseille_geoshapes = gpd.read_file(filepath)
    res_marseille = df[df['code_insee_commune'] == '13055']
    res = df[df['code_insee_commune'] != '13055']
    marseille_geoshapes['geo_point'] = marseille_geoshapes.geometry.centroid
    marseille_geoshapes['latitude'] = marseille_geoshapes.geometry.centroid.x.astype(str)
    marseille_geoshapes['longitude'] = marseille_geoshapes.geometry.centroid.y.astype(str)

    marseille_geoshapes['latitude_longitude'] = marseille_geoshapes['latitude'] + ', ' + marseille_geoshapes['longitude']
    marseille_geoshapes.rename(columns={'bureau': 'code_bdv'}, inplace=True)
    res_marseille['code_bdv'] = res_marseille['code_bdv'].apply(lambda x: change_code_bdv(x, '13055', '132'))
    res_marseille['nom_commune_bdv'] = res_marseille.apply(lambda x: change_nom_commune(x, '13055'), axis=1)
    res_marseille['code_insee_commune'] = res_marseille.apply(lambda x: change_code_insee(x), axis=1)
    filepath = '/Users/eugenie.ly/Documents/La REM/données/fonds de carte/bureaux de vote/marseille_bv_arrdt_circ_secteur_2013_lzhs3wd.csv'
    marseille_adresses = pd.read_csv(filepath, sep=';',
                                     dtype={'BV': str, 'Lbl Bureau Vote': str, 'Lbl Etablissement': str,
                                            'Arrdt Code': str, 'CI': str, 'Canton': str, 'Sect': str,
                                            'Adr1': str, 'Adr2': str})
    res_marseille_jointure_intermediaire = pd.merge(res_marseille, marseille_geoshapes, on='code_bdv', how='left')
    marseille_adresses['code_bdv'] = marseille_adresses['BV'].apply(lambda x: marseille_id_bdv_to_code_bdv(x, '132'))
    res_marseille_jointure = pd.merge(res_marseille_jointure_intermediaire, marseille_adresses, on='code_bdv',
                                      how='left')
    res_marseille_jointure['nom_bdv'] = res_marseille_jointure['Lbl Bureau Vote'].apply(
        lambda x: marseille_nom_bdv(x)).str.title()
    res_marseille_jointure['adresse_bdv'] = res_marseille_jointure.apply(lambda x: marseille_adresse_bdv(x), axis=1).str.title()
    res_marseille_jointure['code_postal'] = res_marseille_jointure.code_bdv.apply(lambda x: '130' + x[-4:-2])
    return res.append(res_marseille_jointure[res.columns.tolist()], ignore_index=True)


def info_commune(df, nom_election, date_election):
    filepath_pop_muni = "/Users/eugenie.ly/Documents/La REM/pôle politique/fiche localité/input/recensement_2016.csv"
    pop_muni = pd.read_csv(filepath_pop_muni, sep=';',
                           dtype={'code_insee_departement': str, 'code_insee_canton': str,
                                  'code_insee_arrondissement': str, 'code_insee_commune': str, 'code_insee_region': str,
                                  'nom_commune': str})
    pop_muni.rename(columns={'popMuni': 'population_commune'}, inplace=True)
    pop_muni['code_insee_commune'] = pop_muni['code_insee_departement'] + pop_muni['code_insee_commune']
    pop_muni = pop_muni[['code_insee_commune', 'population_commune']]

    res2 = pd.merge(df, pop_muni, on='code_insee_commune', how='left')
    res2['nom_election'] = nom_election
    res2['date_election'] = date_election
    data = json.load(
        open('/Users/eugenie.ly/Documents/La REM/données/fonds de carte/communes/output/geoshapes_communes.geojson'))
    l_code_insee = []
    l_latitude_longitude = []
    l_code_postal = []
    for i in range(len(data['features'])):
        l_code_insee.append(data['features'][i]['properties']['town_insee'])
        l_latitude_longitude.append(data['features'][i]['properties']['latitude_longitude'])
        l_code_postal.append(data['features'][i]['properties']['code_postal'])
    geoshapes = pd.DataFrame(
        {'code_insee_commune': l_code_insee,
         'latitude_longitude_commune': l_latitude_longitude,
         'code_postal': l_code_postal
         })
    if 'code_postal' in res2.columns.tolist():
        res3 = pd.merge(res2.drop(columns=['code_postal']), geoshapes, on='code_insee_commune', how='left')
    else:
        res3 = pd.merge(res2, geoshapes, on='code_insee_commune', how='left')

    filepath = '/Users/eugenie.ly/Documents/La REM/pôle politique/fiche localité/input/departement.csv'
    regions = pd.read_csv(filepath, sep=';',
                          dtype={'code_insee_region': str, 'nom_region': str,
                                 'code_insee_departement': str, 'nom_departement': str})
    l = ['nom_region', 'nom_departement']
    for c in l:
        regions[c] = regions[c].str.title()
    return pd.merge(res3, regions[['code_insee_region', 'nom_region', 'code_insee_departement']], on='code_insee_departement', how='left')

