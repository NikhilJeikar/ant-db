use crate::backend::schema::{DataType, DecodedData};
use crate::backend::core::table::CellStructure;
use crate::backend::core::database::SchemaManager;
use crate::backend::handler::setup;
mod backend;

fn main() {
    // Example usage of the database system

    let (_internal_state_manager, _wal_manager, mut db) = setup();

    let table_id = match db.create_table("Users".to_string()) {
        Ok(id) => id,
        Err(e) => {
            match db.get_table_id("Users".to_string()) {
                Ok(table_id) => {
                    println!("Table 'Users' already exists with ID: {}", table_id);
                    table_id
                },
                Err(_) => {
                    println!("Failed to create or find 'Users' table: {}", e);
                    return;
                }
            }
        }
    };
    
    {
        let _ = db.create_column(table_id, "ID".to_string(), DataType::IntegerI128, Vec::new());
        let _ = db.create_column(table_id, "Name".to_string(), DataType::String, Vec::new());
        let _ = db.create_column(table_id, "Age".to_string(), DataType::IntegerU8, Vec::new());
        let _ = db.create_column(table_id, "IsActive".to_string(), DataType::Boolean, Vec::new());

        for i in 1u32..=10u32 {
            let _ = db.insert_rows(table_id, vec![
                    vec![
                        CellStructure {
                            column_id: 1,
                            data: DecodedData::IntegerI128(i as i128),
                        },
                        CellStructure {
                            column_id: 2,
                            data: DecodedData::String(format!("User{}", i)),
                        },
                        CellStructure {
                            column_id: 3,
                            data: DecodedData::IntegerU8((20 + (i % 30)) as u8),
                        },
                        CellStructure {
                            column_id: 4,
                            data: DecodedData::Boolean(i % 2 == 0),
                        },
                    ]
                ]);
        }

        println!("Table '{}' has {} rows.", db.get_table_schema(table_id).unwrap().name, db.get_table_size(table_id).unwrap());
        // Example: get the row with ID 10 (assuming DecodedData::IntegerI128 exists)
        let search = db.get_row(table_id, 10);
        println!("Row with ID 10: {:?}", search);
        println!("Table Schema: {:?}", db.get_table_schema(table_id).unwrap().columns);
        println!("Dropping entry with ID 10: {:?}", db.delete_rows(table_id, vec![10]));
        println!("Row with ID 10 after deletion: {:?}", db.get_row(table_id, 10));
        println!("Table '{}' has {} rows after deletion.", db.get_table_schema(table_id).unwrap().name, db.get_table_size(table_id).unwrap());
    }
    
    println!("All tables in the database: {:?}", db.list_tables());
    println!("Table Schema after deletion: {:?}", db.get_table_schema(table_id));
}
