//
//  IOSDatabasesApp.swift
//  IOSDatabases
//
//  Created by Tom Ebergen on 8/10/23.
//

import SwiftUI

@main
struct IOSDatabasesApp: App {
    let persistenceController = PersistenceController.shared

    var body: some Scene {
        WindowGroup {
            ContentView()
                .environment(\.managedObjectContext, persistenceController.container.viewContext)
        }
    }
}
