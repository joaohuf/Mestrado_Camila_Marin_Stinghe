import pandas as pd
import geopandas as gpd
import shapefile
from shapely.ops import cascaded_union
from shapely.geometry import Point, Polygon
import shapefile as sf


def gen_polygons_from_shape(shape):
    """Gera polígonos a partir das partes do shape.

    Args:
        shape (shapefile.Shape): shape a converter
    """
    offset_parts = list(shape.parts[1:]) + [len(shape.points)]
    for part_start, part_end in zip(shape.parts, offset_parts):
        yield Polygon(shape.points[part_start:part_end])


def is_within_bounds(point, bounding_box):
    """Verifica se o ponto está dentro da região.

    Args:
        point (shapely.geometry.Point): ponto a verificar
        bounding_box (list): lista de coordenadas (sf.Shape.bbox)

    Returns:
        bool: True se estiver dentro, Falso caso contrário
    """
    x1, y1, x2, y2 = bounding_box
    return (x1 <= point.x <= x2) and (y1 <= point.y <= y2)


def is_within_shape(point, shape):
    """Verifica se o shape contém o ponto dado.

    Args:
        point (shapely.geometry.Point): ponto a verificar
        shape (sf.Shape): shape a verificar (pyshp)

    Returns:
        bool: True se o ponto está dentro, Falso caso contrário
    """
    # Verifica primeiro se o ponto está fora da bounding box (mais rápido!)
    if not is_within_bounds(point, shape.bbox):
        return False

    # Verifica então se alguma das partes do polígono contém o ponto
    for poly in gen_polygons_from_shape(shape):
        if point.within(poly):
            return True

    return False


def gen_polygons_from_shape(shape):
    """Gera polígonos a partir das partes do shape.

    Args:
        shape (shapefile.Shape): shape a converter
    """
    offset_parts = list(shape.parts[1:]) + [len(shape.points)]
    for part_start, part_end in zip(shape.parts, offset_parts):
        yield Polygon(shape.points[part_start:part_end])


def find_basin_adaptado(shapefile_path, reach_field, basin_field, x, y, area_field=None, progress_callback=None,
                        return_reach=False):
    """Encontra a bacia dadas as coordenadas (x, y) da exutória.

    O algorítmo primeiramente procura a ottobacia na qual o ponto se encontra,
    verificando se está dentro do polígono. Em seguida, é gerado o polígono
    completo da bacia, buscando ottobacias com mesmo curso d'água (reach_field),
    nas quais o código da bacia é maior do que a bacia encontrada inicialmente
    (basin_field).

    Pode ser passada uma função para reportar o progresso, a qual deve receber
    um número inteiro.

    Args:
        shapefile_path (str): caminho do shapefile das ottobacias
        reach_field (str): nome do campo do código do curso d'água
        basin_field (str): nome do campo do código da bacia
        x (float): coordenada x, no mesmo sistema do shapefile
        y (float): coordenada y, no mesmo sistema do shapefile
        area_field (str): nome do campo do valor da área (se disponível)
        progress_callback (callable, optional): Defaults to None. Função para reportar progresso

    Returns:
        shapely.geometry.Polygon: polígono da bacia encontrada, None caso contrário
        area: float com o valor da soma das areas dos poligonos da bacia utilizados (só retorna isso se area_field != None)

    Examples:
        poli, area = find_basin(caminho, cursodag, bacia, cox, coy, arean)
        poli = find_basin(caminho, cursodag, bacia, cox, coy)
    """

    point = Point(x, y)
    current_progress = 0
    if progress_callback is None:
        progress_callback = lambda i: None

    with sf.Reader(shapefile_path) as shp:
        # Pega os campos do shape, excluindo o primeiro (é uma flag)
        fields = [f[0] for f in shp.fields[1:]]
        feature_count = len(shp)

        # Verifica os polígonos até encontrar a bacia
        for i, sr in enumerate(shp.iterShapeRecords()):
            if is_within_shape(point, sr.shape):
                break

            progress = ((i + 1) * 50) // feature_count
            if current_progress < progress:
                current_progress += 1
                progress_callback(current_progress)

        else:
            # O loop não foi parado, então não foi encontrada nenhuma bacia
            progress_callback(100)
            if area_field != None:
                return None, None
            else:
                return None

    # Salva informações sobre o curso d'água e bacia encontrados
    reach_index = fields.index(reach_field)
    basin_index = fields.index(basin_field)
    if area_field != None:
        area_index = fields.index(area_field)
        selected_area = sr.record[area_index]
    selected_reach = sr.record[reach_index]
    selected_basin = sr.record[basin_index]
    selected_basin_int = int(selected_basin)
    selected_basin_length = len(selected_basin)

    # Abre novamente o shapefile para encontrar a bacia completa
    basin_polygons = [];
    area = 0.0
    with sf.Reader(shapefile_path) as shp:
        for i, sr in enumerate(shp.iterShapeRecords()):
            # O código do curso d'água deve iniciar com o encontrado
            reach = sr.record[reach_index]
            if reach.startswith(selected_reach):
                # Deixa o código da bacia no mesmo tamanho para poder comparar
                basin = sr.record[basin_index][:selected_basin_length].ljust(selected_basin_length, '0')
                basin_int = int(basin)

                # O código da bacia deve ser maior ou igual que o da bacia encontrada
                if basin_int >= selected_basin_int:
                    for poly in gen_polygons_from_shape(sr.shape):
                        basin_polygons.append(poly.buffer(0))
                        if area_field != None:
                            try:
                                area += float(sr.record[area_index].replace(',', '.'))
                            except AttributeError:
                                area += sr.record[area_index]

            progress = 50 + ((i + 1) * 50) // feature_count
            if current_progress < progress:
                current_progress += 1
                progress_callback(current_progress)

    # Retorna a união de todos os polígonos da bacia
    if area_field != None and return_reach:
        return cascaded_union(basin_polygons), area, selected_reach
    elif area_field != None:
        return cascaded_union(basin_polygons), area
    else:
        return cascaded_union(basin_polygons)


def save_polygons(shapefile_path, *polygons):
    """Salva polígonos em um shp

    Args:
        shapefile_path (str): caminho do shapefile
        polygons (shapely.geometry.Polygon): polígonos a salvar
    """
    if polygons:
        with shapefile.Writer(shapefile_path) as shp:
            shp.field('id', 'N')
            for i, poly in enumerate(polygons):
                shp.record(i)
                shp.poly([poly.exterior.coords])


# Arquivo para salvar os dados
COBACIAS = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_3/shps/OTTOBACIAS.shp'
SHP_QMAX = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_2/shps/H_Integrada_AEG_Enquadramento.shp'
Dir_save = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_3/resultados/'
# converte de m3/h pra l/s
F_CONVERCAO = 3.6

# CAPTACAO
POINTS_SHP = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_3/shps/CaptacaoTibagi.shp'
Dir_save_pol = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_3/resultados/poligons_captacao/'
f_saida = 'Bacias_Captacao'
name_vazoes = 'VAZAO_OUTO'

# LANCAMENTO
# POINTS_SHP = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_2/shps/LancamentoTibagi.shp'
# Dir_save_pol = '/media/joao/HD-jao/bacia_iguacu_Camila/teste_2/resultados/poligons_lancamento/'
# f_saida = 'Bacias_Lancamento'
# name_vazoes = 'EFLO_OT_E1'

# Dataframe com os pontos a serem coletados - TROCAR DEPOIS
dt = gpd.read_file(POINTS_SHP)
dt = dt[['OBJECTID', 'geometry', 'COD_OTTO', name_vazoes]]
dt[name_vazoes] = dt[name_vazoes] / F_CONVERCAO

dt_Qmax = gpd.read_file(SHP_QMAX)
dt_Qmax.index = dt_Qmax['cobacia']
dt_Qmax = dt_Qmax[['areamont_Q']]

dt_subbacias = pd.DataFrame()
dt_subbacias['Longitude'] = dt['geometry'].x
dt_subbacias['Latitude'] = dt['geometry'].y
dt_subbacias['COCURSODAG'] = 'Erro'
dt_subbacias['Q_Outorga (l/s)'] = 'Erro'
dt_subbacias['Q95 (l/s)'] = 'Erro'
dt_subbacias['Poli'] = 'Erro'

for i, (p, COBA_REF) in enumerate(dt[['geometry', 'COD_OTTO']].values):

    print(f'Pontos Executados: {i + 1} de {len(dt_subbacias.values)}')
    # Traça a bacia de drenagem
    poly, area, reach = find_basin_adaptado(COBACIAS, 'cocursodag', 'cobacia', p.x, p.y, area_field='nuareacont',
                                            return_reach=True)

    if poly != None:
        # Pega o CÓDIGO pontos dentro do poligono
        ids = dt['OBJECTID'][dt['geometry'].intersects(poly.buffer(0))].tolist()

        vazao_outo = dt[name_vazoes][dt['OBJECTID'].isin(ids)].sum()
        Q_max = dt_Qmax.loc[str(COBA_REF)].values[0]

        dt_subbacias['Q_Outorga (l/s)'].loc[i] = round(vazao_outo, 2)
        dt_subbacias['Q95 (l/s)'].loc[i] = round(Q_max, 2)
        dt_subbacias['COCURSODAG'].loc[i] = reach
        dt_subbacias['Poli'].loc[i] = poly

        # save_polygons(f'{Dir_save_pol}/Shape_P_{i}', poly)
    else:
        # Caso o ponto fique fora dos poligonos
        with open(f'{Dir_save_pol}Shape_Ponto_{i}_ERRO.txt', "w") as text_file:
            text_file.write("Ponto está fora do shape de bacias")

dt_subbacias = dt_subbacias[dt_subbacias != 'Erro'].dropna()
dt_subbacias['Q_Max (l/s)'] = dt_subbacias['Q95 (l/s)'] / 2
dt_subbacias['Fator de Capacidade'] = dt_subbacias['Q_Outorga (l/s)'] / dt_subbacias['Q_Max (l/s)']

dt_subbacias = gpd.GeoDataFrame(dt_subbacias, geometry='Poli')
dt_subbacias.to_file(f'{Dir_save}{f_saida}.shp', driver='ESRI Shapefile')
