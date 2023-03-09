from datetime import datetime, timedelta
import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore
import os

import logging

import scripts.consts as consts


def get_db_connection():
    server = os.environ.get("DB_SERVER")
    port = os.environ.get("DB_PORT")
    db_name = os.environ.get("DB_NAME")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")

    return psycopg2.connect(
        user=user, password=password, host=server, port=port, database=db_name,
    )


def get_android_bg_v2_stored_dates() -> list[datetime]:
    conn = get_db_connection()
    cursor = conn.cursor()

    # Delete if exists the data for the file date
    sql = f"""
                SELECT DISTINCT
                        received_date::date 
                FROM
                        public.android_bg_v2
                ORDER BY
                        1
            """
    cursor.execute(sql)
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


def remove_outdated_files(total_stored_days: int):
    logging.info("Removing outdated files from the database...")

    file_date_str = (datetime.today() - timedelta(total_stored_days + 1)).strftime(
        "%Y-%m-%d"
    )

    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Delete if exists the data for the file date
        sql = f"""
                    DELETE FROM
                            public.android_bg_v2
                    WHERE
                            received_date::date <= '{file_date_str}'                            
                    """
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logging.info("--> Success")

    except psycopg2.DatabaseError as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()


def save_android_bg_v2_file(file_path: str, table: str = 'public.android_bg_v2'):
    logging.info(f"Saving data in the database. Source file: {file_path}, Table: {table}")
    file_date_str = file_path[-14:-4]

    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Delete if exists the data for the file date
        sql = f"""
                DELETE FROM
                        {table}
                WHERE
                        received_date::date = '{file_date_str}'
                """
        cursor.execute(sql)

        # Import the file data
        column_names_str = ",".join(consts.android_bg_v2_column_names)

        norm_file_path = os.path.abspath(file_path)

        sql = f"""
                COPY {table}(
                    {column_names_str}
                )
                FROM '{norm_file_path}'
                WITH (
                    FORMAT CSV,
                    HEADER,
                    DELIMITER ',',
                    QUOTE '\"',
                    ESCAPE '''',
                    FORCE_NULL(
                        {column_names_str}
                    )
                )
                """
        cursor.copy_expert(sql, open(norm_file_path, "r"))
        conn.commit()
        conn.close()
        logging.info("--> Success")

    except psycopg2.DatabaseError as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()



def update_android_bg_v2__ext_dev_table():
    logging.info("Updating table: android_bg_v2__ext_dev")

    dev_regs_count_percent = 10

    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        sql = f"""
                TRUNCATE TABLE public.android_bg_v2__ext_dev;
                
                INSERT INTO
                        public.android_bg_v2__ext_dev
                SELECT        
                        id,
                        received_date::date AS date,
                        report_id,
                        trigger_name,
                        result_date,
                        received_date,
                        received_date_local,
                        time_zone_name,
                        device_id,
                        device_model,
                        device_manufacturer,
                        device_model_raw,
                        device_manufacturer_raw,
                        device_brand_raw,
                        os_version,
                        app_version,
                        connection_type,
                        is_airplane_mode,
                        is_network_roaming,
                        is_international_roaming,
                        number_registered_networks,
                        number_unregistered_networks,
                        sim_operator_name,
                        raw_sim_operator_name,
                        sim_operator_mcc_code,
                        sim_operator_mnc_code,
                        network_operator_name,
                        network_operator_mcc_code,
                        network_operator_mnc_code,
                        client_latitude,
                        client_longitude,
                        altitude,
                        location_accuracy,
                        location_age,
                        location_type,
                        country,
                        region,
                        subregion,
                        locality,
                        place_type,
                        postal_code,
                        computed_cellular_generation,
                        tac,
                        pci,
                        cell_identifier,
                        lte_enodeb,
                        rnc_id,
                        cell_id,
                        arfcn,
                        uarfcn,
                        earfcn,
                        is_primary_cell,
                        rsrp,
                        rsrq,
                        rssi,
                        rssnr,
                        timing_advance,
                        cqi,
                        wifi_enabled,
                        wifi_state,
                        wifi_rssi,
                        wifi_frequency,
                        wifi_channel,
                        app_foreground,
                        azimuth,
                        battery_level,
                        battery_level_max,
                        battery_plugged,
                        battery_present,
                        battery_status,
                        battery_technology,
                        battery_temperature,
                        battery_voltage,
                        device_idle_mode,
                        humidity,
                        humidity_accuracy,
                        light_lx,
                        light_accuracy,
                        pitch,
                        power_interactive,
                        power_save_mode,
                        pressure_accuracy,
                        pressure_mbar,
                        temp_celsius,
                        temp_accuracy,
                        grant_billing,
                        grant_internet,
                        grant_network_state,
                        grant_phone_state,
                        grant_fine_location,
                        grant_coarse_location,
                        grant_background_location,
                        grant_wifi_state,
                        grant_boot_completed,
                        valid_device_check,
                        location_check,
                        radio,
                        service_state,
                        sim_state,
                        sim_count,
                        guid,
                        cell_bandwidth,
                        vertical_accuracy,
                        nr_ss_rsrp,
                        nr_ss_rsrq,
                        nr_ss_sinr,
                        nr_csi_rsrp,
                        nr_csi_rsrq,
                        nr_csi_sinr,
                        nr_level,
                        nr_asu,
                        nr_arfcn,
                        nr_nci,
                        nr_pci,
                        nr_tac,
                        nr_mcc,
                        nr_mnc,
                        nr_state,
                        nr_frequency_range,
                        is_nr_available,
                        is_nr_telephony_sourced,
                        is_using_carrier_aggregation,
                        chipset_name,
                        chipset_manufacturer,
                        cell_bandwidths,
                        is_access_network_technology_nr,
                        device_tac,
                        downstream_bandwidth_kbps,
                        wifi_channel_width,
                        has_bg_location_permission,
                        has_cellular_service,
                        upstream_bandwidth_kbps,
                        gsm_additional_plmns,
                        tdscdma_additional_plmns,
                        wcdma_additional_plmns,
                        lte_additional_plmns,
                        lte_bands,
                        nr_additional_plmns,
                        nr_bands,
                        gsm_rssi,
                        wcdma_ecno,
                        wifi_rx_link_speed,
                        wifi_max_supported_rx_link_speed,
                        wifi_max_supported_tx_link_speed,
                        wifi_passpoint_fqdn,
                        wifi_passpoint_provider_name,
                        wifi_carrier_name,
                        wifi_standard,
                        wifi_is_2_4GHz_band_supported,
                        wifi_is_6GHz_band_supported,
                        wifi_is_60GHz_band_supported,
                        current_thermal_status,
                        thermal_headroom,
                        alt_sim_operator_name,
                        alt_raw_sim_operator_name,
                        alt_sim_operator_mcc_code,
                        alt_sim_operator_mnc_code,
                        data_activity,
                        data_state,
                        display_state,
                        is_concurrent_voice_data_supported,
                        is_data_capable,
                        is_data_enabled,
                        is_data_connection_allowed,
                        is_data_roaming_enabled,
                        has_icc_card,
                        is_world_phone,
                        is_multi_sim_supported,
                        active_modem_count,
                        supported_modem_count,
                        override_network_type,
                        lac,
                        psc
                FROM
                        public.android_bg_v2
                WHERE
                        random() < {dev_regs_count_percent/100};
                """
        cursor.execute(sql)
        conn.commit()
        conn.close()
        logging.info("--> Success")

    except psycopg2.DatabaseError as error:
        logging.error(error)

    finally:
        if conn is not None:
            conn.close()
