from azure.storage.blob import BlobServiceClient
import geopandas as gpd
import plotly.graph_objects as go
import plotly.io as pio

def load_raw(
        work_dir: str,
        data_config: dict,
        cloud: bool = False,
        overwrite: bool = False
    ) -> gpd.GeoDataFrame:
    """
    Description
    -----------
    Load raw geographic census data.

    Parameters
    ----------
    - work_dir : str
        Working directory.
    - data_config : dict
        Dictionary with data configuration.
    - cloud : bool = False
        Use cloud data.
    - overwrite : bool = False
        Overwrite data if it exists.

    Returns
    -------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    """

    if cloud:
        conn_str = data_config['conn_str']
        gdf = load_cloud_data(conn_str)
    else:
        gdf = load_local_data(
            work_dir,
            data_config['data_year'],
            data_config['data_topic'],
            data_config['geo_area'],
            data_config['gda_spec'],
            data_config['gda_type']
        )

    return gdf

def load_pro(
        work_dir: str,
        data_config: dict,
        cloud: bool = False,
        overwrite: bool = False
    ) -> gpd.GeoDataFrame:
    """
    Description
    -----------
    Load geographic census data from a local file.

    Parameters
    ----------
    - work_dir : str
        Working directory.
    - data_config : dict
        Dictionary with data configuration.
    - cloud : bool = False
        Use cloud data.
    - overwrite : bool = False
        Overwrite data if it exists.

    Returns
    -------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    """

    return gdf

def process_data(
        gdf: gpd.GeoDataFrame,
        gdf_column: dict,
        data_year: int = 2021,
        gda_type: str = 'SA4'
    ) -> gpd.GeoDataFrame:
    """
    Description
    -----------
    Process geographic census dataframe.

    Parameters
    ----------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    - gdf_column : dict
        Dictionary with column info.
    - data_year : int = 2021
        Census data year (2016, 2021, ect.).
    - gda_type : str = 'SA4'
        Type of digital geo bounds to use (LGA, SA2, ect.).

    Returns
    -------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    """

    # Create columns
    gdf_columns = [
        {
            'name': f'{gda_type}_NAME_{data_year}',
            'rename': 'Location',
            'type': 'str'
        },
        gdf_column,
        {
            'name': 'geometry',
            'rename': 'geometry',
            'type': 'geometry'
        }
    ]

    # Filter data and columns
    gdf = gdf[
        gdf['geometry'] != None
    ]
    gdf = gdf.reset_index(drop=True)

    columns = [x['name'] for x in gdf_columns]
    gdf = gdf[columns]

    # Update names and types
    column_names = {x['name']: x['rename'] for x in gdf_columns}
    gdf = gdf.rename(columns=column_names)

    column_types = {x['rename']: x['type'] for x in gdf_columns}
    gdf = gdf.astype(column_types)

    return gdf

def create_figure(
        gdf: gpd.GeoDataFrame,
        gdf_column: dict
    ) -> go.Figure:
    """
    Description
    -----------
    Create a choropleth map figure.

    Parameters
    ----------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    - gdf_column : dict
        Dictionary with column info.

    Returns
    -------
    - figure : plotly.graph_objects.Figure
        Plotly figure.
    """

    # Create figure data
    figure_df = gdf.copy()
    figure_df = figure_df.set_index('Location')
    figure_geojson = figure_df.__geo_interface__

    # Create figure
    figure = go.Figure()

    # Create and add choroplethmap trace
    trace = go.Choroplethmapbox(
        name='Census Data',
        geojson=figure_geojson,
        locations=figure_df.index,
        z=figure_df[gdf_column['rename']],
        marker_opacity=0.5,
        hovertemplate= \
            '<b>Location</b>: %{location}<br>'+\
            '<b>'+gdf_column['rename']+'</b>: %{z:.2s}'+\
            '<extra></extra>'
    )
    figure = figure.add_trace(trace)

    # Create and add map layout
    layout = go.Layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(
                lat=-25,
                lon=130
            ),
            zoom=2,
            bounds=dict(
                west=85,
                east=185,
                north=0,
                south=-50
            )
        ),
        autosize=True,
        margin=dict(
            l=0,
            r=0,
            t=0,
            b=0
        ),
        height=650,
        width=1300
    )
    figure = figure.update_layout(layout)

    return figure

def read_figure(
        work_dir: str,
        data_year: int = 2021,
        data_topic: str = 'G01',
        geo_area: str = 'AUST',
        gda_spec: str = 'GDA2020',
        gda_type: str = 'SA4'
    ):
    """
    Description
    -----------
    Read figure from a file.

    Parameters
    ----------
    - work_dir : str
        Working directory.
    - data_year : int = 2021
        Census data year (2016, 2021, ect.).
    - data_topic : str = 'G01'
        Census data topic id (G01, G40, ect.).
    - geo_area : str = 'AUST'
        Census data geographic area (AUST, QLD, ect.).
    - gda_spec : str = 'GDA2020'
        Geographic digital boundary specification (GDA94, GDA2020, ect.).
    - gda_type : str = 'SA4'
        Type of digital geo bounds to use (LGA, SA2, ect.).
    
    Returns
    -------
    - figure : plotly.graph_objects.Figure
        Plotly figure.
    """

    # Set file name
    filename = \
        f"{data_topic}_{gda_type}_{data_year}_{geo_area}_{gda_spec}"

    # Load figure
    figure = pio.read_json(
        f"{work_dir}/figure/{filename}.json",
    )

    return figure

def save_figure(
        work_dir: str,
        figure: go.Figure,
        file_type: str = 'json',
        data_year: int = 2021,
        data_topic: str = 'G01',
        geo_area: str = 'AUST',
        gda_spec: str = 'GDA2020',
        gda_type: str = 'SA4'
    ):
    """
    Description
    -----------
    Save figure to a file.

    Parameters
    ----------
    - work_dir : str
        Working directory.
    - figure : plotly.graph_objects.Figure
        Plotly figure.
    - file_type : str = 'json'
        File type to save the figure (html, json).
    - data_year : int = 2021
        Census data year (2016, 2021, ect.).
    - data_topic : str = 'G01'
        Census data topic id (G01, G40, ect.).
    - geo_area : str = 'AUST'
        Census data geographic area (AUST, QLD, ect.).
    - gda_spec : str = 'GDA2020'
        Geographic digital boundary specification (GDA94, GDA2020, ect.).
    - gda_type : str = 'SA4'
        Type of digital geo bounds to use (LGA, SA2, ect.).

    Returns
    -------
    - None : None
        None.
    """

    # Set file name
    filename = \
        f"{data_topic}_{gda_type}_{data_year}_{geo_area}_{gda_spec}"

    # Write figure
    if file_type == 'html':
        pio.write_html(
            figure,
            f"{work_dir}/figure/{filename}.html"
        )
    elif file_type == 'json':
        pio.write_json(
            figure,
            f"{work_dir}/figure/{filename}.json",
        )

    return None

def load_local_data(
        work_dir: str,
        data_year: int = 2021,
        data_topic: str = 'G01',
        geo_area: str = 'AUST',
        gda_spec: str = 'GDA2020',
        gda_type: str = 'SA4'
    ) -> gpd.GeoDataFrame:
    """
    Description
    -----------
    Load geographic census data from a local file.

    Parameters
    ----------
    - work_dir : str
        Working directory.
    - data_year : int = 2021
        Census data year (2016, 2021, ect.).
    - data_topic : str = 'G01'
        Census data topic id (G01, G40, ect.).
    - geo_area : str = 'AUST'
        Census data geographic area (AUST, QLD, ect.).
    - gda_spec : str = 'GDA2020'
        Geographic digital boundary specification (GDA94, GDA2020, ect.).
    - gda_type : str = 'SA4'
        Type of digital geo bounds to use (LGA, SA2, ect.).

    Returns
    -------
    - gdf : geopandas.GeoDataFrame
        Geopandas dataframe.
    """

    # Set filename and layer
    folder = \
        f'Geopackage_{data_year}_{data_topic}_{geo_area}_{gda_spec}'
    file = \
        f'{data_topic}_{geo_area}_{gda_spec}.gpkg'

    filename = \
        f'{work_dir}/raw/{folder}/{file}'
    layer = \
        f'{data_topic}_{gda_type}_{data_year}_{geo_area}'

    # Load data
    gdf = gpd.read_file(
        filename=filename,
        layer=layer
    )

    return gdf

def load_cloud_data(conn_str):

    # Create a BlobServiceClient object using the connection string
    blob_service_client = \
        BlobServiceClient.from_connection_string(
            conn_str
        )

    # Get the container client object
    container_name = "australian-census-data"
    container_client = \
        blob_service_client.get_container_client(
            container_name
        )

    # List all blobs in the container
    blobs = container_client.list_blobs()

    # Print the names of all blobs
    print(f"Blobs in the container '{container_name}':")
    for blob in blobs:
        print(blob.name)

    return blobs