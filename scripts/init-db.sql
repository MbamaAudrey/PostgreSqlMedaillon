
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


COMMENT ON SCHEMA bronze IS 'Zone de stockage des données brutes (Raw Data) extraites de T24.';
COMMENT ON SCHEMA silver IS 'Zone de stockage des données nettoyées, typées et structurées.';
COMMENT ON SCHEMA gold IS 'Zone de stockage des données agrégées et modélisées pour le reporting final.';
