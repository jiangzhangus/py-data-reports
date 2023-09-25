# https://trello.com/c/GFLFwkog/6-script-to-download-all-parsed-genomics-data
# PN, Hereditary disease risk, PGX, PRS, simple traits

import csv
import psycopg2

from typing import List
from logger import logger
from account import longevity

# Keep update this value with last finished index id. this is 1-based
last_finished_line_num = 0
input_file = "prism_public_subject.csv"


# 3	  gcr-simple-traits
# 17  gcr-pharmacogenomics-findings
# 69  gcr-polygenic-risk-findings
# 135 gcr-hereditary-disease-risk


# PostgreSQL database connection parameters
db_params = {
    "dbname": longevity["db_name"],
    "user": longevity["db_user"],
    "password": longevity["db_pass"],
    "host": longevity["db_host"],
    "port": 5432
}


query = """
SELECT
    o.created_on as creation,
    rt.report_name,
    r.report_metadata as metadata,
    r.content as content
FROM
    "order" o
LEFT JOIN
    report r on o.id = r.order_id
LEFT JOIN
    patient p on o.patient_id = p.id
LEFT JOIN
    report_type rt on r.report_type_id = rt.id
WHERE
    p.hli_subject_id = %(hli_subject_id)s
    AND
    r.soft_deleted = false
    AND
    r.report_type_id in (3, 17, 69, 135)

ORDER BY o.created_on
;
""".replace("\n", " ")


def query_data(pg_curser: object, hli_subject_id: str) -> List[List[str]]:
    data = {"hli_subject_id": hli_subject_id}
    pg_curser.execute(query, data)
    gcr_info = pg_curser.fetchall()
    return gcr_info


def process_input_row(cursor, input_row):
    cur_line_num = int(input_row[0])
    hli_subject_id = input_row[1]
    client_subject_id = input_row[2]

    gcr_info = query_data(cursor, hli_subject_id)
    if not gcr_info:
        logger.warning(f"No information found in database for hli_subject_id: {hli_subject_id}")
        return False

    save_count = 0
    for data_row in gcr_info:
        creation, report_name, metadata, content = data_row
        # if report_name != "gcr-polygenic-risk-findings":  # TODO: debug, remove this
        #     continue

        if report_obj := out_data.get(report_name):
            output_rows = report_obj.build_output_rows(client_subject_id, creation, metadata, content)
            report_obj.write_rows(output_rows)
            save_count += 1

    if save_count > 0:
        logger.info(f"finished input line number {cur_line_num} for hli_subject_id: {hli_subject_id}")
    else:
        logger.warning(f"No information found for hli_subject_id: {hli_subject_id} on input line {cur_line_num}")


class ReportBase(object):
    def __init__(self) -> None:
        pass

    def build_output_rows(self, client_subject_id, creation, metadata, content):
        return []

    def write_headers(self):
        self.output_file_writer.writerow(self.columns)

    def write_rows(self, output_rows):
        self.output_file_writer.writerows(output_rows)


class HereditaryDiseaseRiskReport(ReportBase):
    name = "Hereditary Disease Risk"
    pdf_table = "Variant Summary"
    report_name = "gcr-hereditary-disease-risk"
    column_index_map = {
        "PN": 0,
        "CREATION": 1,
        "HEALTH CATEGORY": 2,
        "GENE": 3,
        "VARIANT": 4,
        "ZYGOSITY": 5,
        "CLASSIFICATION": 6,
        "DISEASE ASSOCIATED": 7,
        "INHERITANCE": 8
    }
    output_file = "out_hereditary_disease.csv"

    def __init__(self) -> None:
        fd = open(self.output_file, "at", newline='', encoding='utf-8')
        self.output_file_writer = csv.writer(fd)
        self.columns = self.column_index_map.keys()

    def build_output_rows(self, client_subject_id, creation, metadata, content):
        metadata_list = metadata["content_metadata"]
        output_rows = []
        for metadata_item in metadata_list:
            row = [None] * len(self.columns)  # init an array
            row[0] = client_subject_id
            row[1] = str(creation.date())
            keys = metadata_item.keys()
            for key in keys:
                index = self.column_index_map[key.upper().replace('_', ' ')]
                if key == 'variant':
                    row[index] = "   ".join(metadata_item[key].values())
                else:
                    row[index] = metadata_item[key].replace('_', ' ')

            output_rows.append(row)
        return output_rows


class PhamacogenomicsFindingsReport (ReportBase):
    name = "Pharmacogenomic Findings (PGX)",
    pdf_table = "Drugs Potentially Impacted",
    report_name = 'gcr-pharmacogenomics-findings'
    column_index_map = {
        "PN": 0,
        "CREATION": 1,
        "THERAPEUTIC": 2,
        "GENE": 3,
        "GENOTYPE": 4,
        "PHENOTYPE": 5,
        "DRUG": 6,
        "COMMENTS": 7
    }
    output_file = "out_pharmacogenomics_findings.csv"

    def __init__(self) -> None:
        self.output_file_writer = csv.writer(open(self.output_file, "at", newline='', encoding='utf-8'))
        self.columns = self.column_index_map.keys()

    def build_output_rows(self, client_subject_id, creation, metadata, content):
        metadata_list = metadata["content_metadata"]["Therapeutic"]
        output_rows = []
        for metadata_item in metadata_list:
            row = [None] * len(self.columns)  # init an array
            row[0] = client_subject_id
            row[1] = str(creation.date())
            row[2] = metadata_item["therapeutic_category"]
            row[3] = metadata_item["gene"]
            row[4] = metadata_item["genotype"]
            row[5] = metadata_item["phenotype"]
            row[6] = metadata_item["drugname"]
            row[7] = metadata_item["response"]
            output_rows.append(row)
        return output_rows


class PolygenicRiskFindingsReport(ReportBase):
    report_name = "gcr-polygenic-risk-findings"
    title = "Gene and Phenotype Findings - PRS",
    pdf_table = "Gene and Phenotype Findings",
    column_index_map = {
        "PN": 0,
        "CREATION": 1,
        "HEALTH CATEGORY": 2,
        "DISEASES": 3
    }
    output_file_90u = "out_polygenic_risk_90u.csv"  # 90 percentile and above
    output_file = "out_polygenic_risk_90b.csv"  # less than 90 percentile

    def __init__(self) -> None:
        self.output_file_writer90u = csv.writer(open(self.output_file_90u, "at", newline='', encoding='utf-8'))
        self.output_file_writer = csv.writer(open(self.output_file, "at", newline='', encoding='utf-8'))

        self.columns = self.column_index_map.keys()
        self.columns_90u = ["PN", "CREATION", "DISEASE", "RESULT", "ABOUT THE DISEASE"]

    def _build_mappings(self, content_metadata, content):
        category2diseases = {}
        disease2Interpretation = {}
        disease_info = {}

        for trait, details in content_metadata.items():
            category = details["TraitCategory"]
            interpretation = details.get("TraitInterpretation", "")

            if category not in category2diseases:
                category2diseases[category] = []

            disease2Interpretation[trait] = interpretation
            category2diseases[category].append(trait)

        for disease, info in content.items():
            disease_info[disease] = {
                "relative_risk_upper": info.get("relative_risk_upper", ''),
                "polygenic_risk_score": info.get("polygenic_risk_score", ''),
                "genetics_risk_percentile": info.get("genetics_risk_percentile", ''),
                "heritability description": info.get("heritability description", ''),
                "disease prevalence description": info.get("disease prevalence description", '')
            }

        return (disease_info, disease2Interpretation, category2diseases)

    def build_output_rows(self, client_subject_id, creation, metadata, content):
        content_metadata = metadata["content_metadata"]
        disease_info, disease2Interpretation, category2diseases = self._build_mappings(content_metadata, content)

        output_rows_90u = []
        output_rows = []

        for disease, info in disease_info.items():
            percentile = info["genetics_risk_percentile"]
            if percentile >= 90:  # high percentile findings (90th and above)
                row = [None] * len(self.columns_90u)
                row[0] = client_subject_id
                row[1] = str(creation.date())
                row[2] = disease
                row[3] = disease2Interpretation[disease]
                heritability_description = info["heritability description"]
                prevalence_description = info["disease prevalence description"]
                ext_info = f"{heritability_description}  {prevalence_description}"
                row[4] = ext_info.strip()
                output_rows_90u.append(row)
            else:
                category_list = category2diseases.keys()
                for category in category_list:
                    row = [None] * len(self.columns)  # init an array
                    row[0] = client_subject_id
                    row[1] = str(creation.date())
                    row[2] = category

                    # reconstruct with extra information for each disease
                    diseases_ext = []  # with percentile and relative Risk numbers
                    for disease in category2diseases[category]:
                        _percentile = disease_info[disease]["genetics_risk_percentile"]
                        _relative_risk = disease_info[disease].get("relative_risk_upper")
                        if _relative_risk is None or _relative_risk == 'null':
                            _relative_risk = 'N/A'
                        ext_info = f"{disease} (Percentile: {_percentile}  Relative Risk: {_relative_risk})"
                        diseases_ext.append(ext_info)

                    row[3] = "   ".join(diseases_ext).strip()
                    output_rows.append(row)

        self.output_rows_90u = output_rows_90u
        return output_rows

    def write_headers(self):
        self.output_file_writer.writerow(self.columns)
        self.output_file_writer90u.writerow(self.columns_90u)

    def write_rows(self, output_rows):
        self.output_file_writer.writerows(output_rows)
        self.output_file_writer90u.writerows(self.output_rows_90u)


class SimpleTraitsReport(ReportBase):
    report_name = "gcr-simple-traits"
    name = "Nutritional and Wellness Traits"
    pdf_table = "Nutrition"
    column_index_map = {
        "PN": 0,
        "CREATION": 1,
        "TRAITS": 2,
        "RESULTS": 3,
        "GENOTYPE": 4,
    }
    output_file = "out_simple_traits.csv"

    def __init__(self) -> None:
        self.output_file_writer = csv.writer(open(self.output_file, "at", newline='', encoding='utf-8'))
        self.columns = self.column_index_map.keys()

    def build_output_rows(self, client_subject_id, creation, metadata, content):
        traits = metadata["content_metadata"]
        rows = []
        for key in traits.keys():
            row = [None] * len(self.columns)  # init an array
            row[0] = client_subject_id
            row[1] = str(creation.date())
            row[2] = traits[key]["TraitName"]  # traits
            variants = traits[key]["VariantSet"]
            row[3] = variants['summary']
            geno_type_values = [variants['gene_symbol'], variants['rs_id'], variants['genotype']]
            row[4] = "   ".join(geno_type_values).strip()
            rows.append(row)
        return rows


out_data = {
    "gcr-hereditary-disease-risk": HereditaryDiseaseRiskReport(),
    "gcr-pharmacogenomics-findings": PhamacogenomicsFindingsReport(),
    "gcr-polygenic-risk-findings": PolygenicRiskFindingsReport(),
    "gcr-simple-traits": SimpleTraitsReport()
}


def main():
    # input fields: ["row_number", "hli_subject_id", "client_subject_id"]
    try:
        with psycopg2.connect(**db_params) as connection:
            with connection.cursor() as cursor, open(input_file, "rt") as input_csv_fd:

                if last_finished_line_num == 0:
                    for _, obj in out_data.items():
                        obj.write_headers()

                csv_reader = csv.reader(input_csv_fd, delimiter=",")
                for row in csv_reader:
                    cur_line_num = int(row[0])
                    if cur_line_num <= last_finished_line_num:
                        logger.info(f"skipping line: {cur_line_num}")
                    else:
                        process_input_row(cursor, row)

    except psycopg2.Error as e:
        logger.error(f"failed to query data: {e}")


if __name__ == "__main__":
    main()
