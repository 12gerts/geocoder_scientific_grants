import os
import re
from decimal import Decimal
from functools import lru_cache

import pandas as pd
from docx import Document
import osmnx as ox
from dataclasses import dataclass

from pdf2docx import Converter
import wikipedia

wikipedia.set_lang("ru")


@dataclass
class TableStruct:
    project_name: int = None
    grant_name: int = None
    year: int = None
    organization: int = None
    organization_short_name: str = None
    lat: float = None
    lon: float = None


def convert_files() -> list:
    files = os.listdir('projects')
    added_files = []

    for pdf_file in files:
        if pdf_file.startswith('.'):
            continue

        docx_file = f'processed_project/{pdf_file[:-4]}.docx'
        if not os.path.exists(docx_file):
            added_files.append(docx_file)
            cv = Converter(f'projects/{pdf_file}')
            cv.convert(docx_file)
            cv.close()

    return added_files


@lru_cache
def get_geocode(organization: str) -> tuple[pd.Series, pd.Series]:
    geocode = ox.geocode_to_gdf(organization)
    return geocode.lat, geocode.lon


@lru_cache
def get_geocode_wiki(organization: str) -> tuple[Decimal, Decimal]:
    page = wikipedia.search(organization)
    return wikipedia.page(page[0]).coordinates


def process_document(document: Document, main_df: pd.DataFrame) -> pd.DataFrame:
    table_struct = TableStruct()

    try:
        year = re.search(r'20\d{2}', document.paragraphs[1].text)
    except IndexError:
        return main_df

    if year:
        table_struct.year = year[0]
    table_struct.grant_name = re.search(r'(?<=«)[^»]*', document.paragraphs[1].text)[0]

    for index, cell in enumerate(document.tables[0].rows[0].cells):
        if re.search(r'Название', cell.text):
            table_struct.project_name = index
        elif re.search(r'Российская организация|Организация', cell.text):
            table_struct.organization = index

    df = pd.DataFrame(columns=[*table_struct.__dict__.keys()])

    for table in document.tables:
        for row in table.rows:
            organization = row.cells[table_struct.organization].text.replace('\n', '')
            search_short_name = re.search(r'(?<=образования).+|(?<=науки).+|(?<=предприятие).+', organization)

            if not search_short_name:
                search_short_name = re.search(
                    r'(?<=учреждение).+|(?<=организация).+|(?<=ответственностью).+', organization)

            short_name = search_short_name[0] if search_short_name else organization

            try:
                lat, lon = get_geocode(short_name)
            except ValueError:
                try:
                    lat, lon = get_geocode_wiki(short_name)
                except (KeyError, IndexError, wikipedia.exceptions.WikipediaException):
                    lat = lon = None

            current_row = pd.DataFrame(
                dict(
                    zip(
                        table_struct.__dict__.keys(),
                        [
                            row.cells[table_struct.project_name].text.replace('\n', ''),
                            table_struct.grant_name,
                            table_struct.year,
                            organization,
                            short_name,
                            lat,
                            lon
                        ]
                    )
                ),
                index=[0]
            )
            df = pd.concat([df, current_row], ignore_index=True)

    df.drop(labels=[0], axis=0, inplace=True)
    return pd.concat([df, main_df], ignore_index=True)


def create_dataframe():
    if not os.path.exists('grants.csv'):
        main_df = pd.DataFrame(columns=[*TableStruct().__dict__.keys()])
    else:
        main_df = pd.read_csv('grants.csv')

    for document in convert_files():
        main_df = process_document(Document(document), main_df)

    main_df.to_csv(r'grants.csv', index=False)


create_dataframe()
