COUNTRY_MAPPINGS = {
    "Russian Federation": "Russia",
    "Korea, Dem. People's Rep.": "Korea, North",
    "North Korea": "Korea, North",
    "Timor-Leste": "Timor Leste",
    "Slovak Republic": "Slovakia",
    "Turkiye": "Türkiye",
    "Congo, Rep.": "Congo, Republic",
    "Congo, Dem. Rep.": "Congo, DR",
    "Lao PDR": "Laos",
    "Cabo Verde": "Cape Verde",
    "Venezuela, RB": "Venezuela",
    "Iran, Islamic Rep.": "Iran",
    "United States": "United States of America",
    "Syrian Arab Republic": "Syria",
    "Brunei Darussalam": "Brunei",
    "Korea, Rep.": "Korea, South",
    "Egypt, Arab Rep.": "Egypt",
}

COUNTRIES_TO_DROP = [
    "Yemen Rep.",
    "Yemen",
    "Yemen, North",
    "Taiwan",
    "Czechoslovakia",
    "German Democratic Republic",
    "Yugoslavia",
    "USSR",
]


NULL_INDICATORS = ["..", "...", "0", "1"]


DB_OPTIONS = {
    "url": "jdbc:postgresql://host.docker.internal:5433/postgres",
    "user": "postgres",
    "password": "testPassword",
    "dbtable": "ms_di",
}
