import PackageDescription

let package = Package(
    name: "Generations",
    dependencies: [
        .Package(url: "https://github.com/OpenKitten/MongoKitten.git", majorVersion: 3)
    ]
)
