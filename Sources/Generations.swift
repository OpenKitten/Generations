import Foundation
@_exported import MongoKitten
@_exported import BSON

public enum GenerationError: Error {
    case invalidDiffGeneration
}

public class GenerationCollection {
    public let states: MongoKitten.Collection
    public let diffs: MongoKitten.Collection
    
    public let name: String
    
    public init(inDatabase database: Database, named bucketName: String = "generations") throws {
        states = database["\(bucketName).states"]
        diffs = database["\(bucketName).diffs"]
        name = bucketName
    }
    
    public func insertGeneration(fromDocument document: Document) throws -> GenerationState {
        let generation = GenerationState.makeGeneration(fromDocument: document, inGenerationCollection: self)
        
        try self.states.insert(generation.state.makeDocument())
        try self.diffs.insert(generation.initialDiff.makeDocument())
        
        return generation.state
    }
    
    public func findState(byId id: ObjectId) throws -> GenerationState? {
        guard let doc = try states.findOne(matching: "_id" == id) else {
            return nil
        }
        
        return GenerationState(document: doc, inGenerationCollection: self)
    }
    
    public func findPrimaryState(inTree tree: ObjectId) throws -> GenerationState? {
        guard let doc = try states.findOne(matching: "tree" == tree && "generation" == Int32(0)) else {
            return nil
        }
        
        return GenerationState(document: doc, inGenerationCollection: self)
    }
    
    public func findState(forGeneration generation: Int32, inTree tree: ObjectId) throws -> Document? {
        guard let doc = try states.findOne(matching: "tree" == tree && "generation" == Int32(0)) else {
            return nil
        }
        
        return try GenerationState(document: doc, inGenerationCollection: self)?.constructState(atGeneration: generation)
    }
}

public struct GenerationDiff: ValueConvertible {
    init?(document: Document) {
        guard let id = document["_id"] as ObjectId?, let diff = document["diff"] as Document?, let generation = document["generation"] as Int32?, let tree = document["tree"] as ObjectId?, let branch = document["branch"] as Int32?, let creation = document["creation"] as Date? else {
            return nil
        }
        
        self.id = id
        self.diff = diff
        self.generation = generation
        self.tree = tree
        self.branch = branch
        self.creation = creation
    }
    
    internal init(diff: Document, generation: Int32, tree: ObjectId, branch: Int32) {
        self.id = ObjectId()
        self.diff = diff
        self.generation = generation
        self.tree = tree
        self.branch = branch
        self.creation = Date()
    }
    
    public func makeBSONPrimitive() -> BSONPrimitive {
        return makeDocument()
    }
    
    let id: ObjectId
    public let diff: Document
    public let generation: Int32
    public let tree: ObjectId
    public let branch: Int32
    public let creation: Date
    
    public func makeDocument() -> Document {
        return [
            "_id": id,
            "diff": diff,
            "generation": generation,
            "tree": tree,
            "branch": branch,
            "creation": creation
        ]
    }
}

public struct GenerationState: ValueConvertible {
    internal init?(document: Document, inGenerationCollection generationCollection: GenerationCollection) {
        guard let id = document["_id"] as ObjectId?, let treeId = document["tree"] as ObjectId?, let object =  document["object"] as Document?, let lastDiff = document["lastDiff"] as Int32? else {
            return nil
        }
        
        self.genCollection = generationCollection
        self.stateIdentifier = id
        self.treeIdentifier = treeId
        self.cachedObject = object
        self.lastDiff = lastDiff
        self.branch = 0
    }
    
    private init(withInitialDocument initialDocument: Document, inGenerationCollection generationCollection: GenerationCollection) {
        self.genCollection = generationCollection
        self.treeIdentifier = ObjectId()
        self.cachedObject = initialDocument
        self.lastDiff = 0
        self.stateIdentifier = ObjectId()
        self.branch = 0
    }
    
    public static func makeGeneration(fromDocument initialDocument: Document, inGenerationCollection genCollection: GenerationCollection) -> (state: GenerationState, initialDiff: GenerationDiff) {
        var state = GenerationState(withInitialDocument: initialDocument, inGenerationCollection: genCollection)
        
        let diff = GenerationDiff(diff: initialDocument, generation: 1, tree: state.treeIdentifier, branch: state.branch)
        
        state.lastDiff = 1
        
        return (state, diff)
    }
    
    public mutating func updateState(updatingWith document: Document) throws -> GenerationDiff {
        self.lastDiff += 1
        
        let diff = GenerationDiff(diff: document, generation: self.lastDiff, tree: self.treeIdentifier, branch: self.branch)
        
        func updateCache(forDocument document: Document, inSubKeys subkeys: [SubscriptExpressionType]) {
            for (key, value) in document {
                var subkeys = subkeys
                subkeys.append(key)
                
                if let document = value as? Document, !document.validatesAsArray() {
                    updateCache(forDocument: document, inSubKeys: subkeys)
                } else {
                    self.cachedObject[raw: subkeys] = value
                }
            }
        }
        
        updateCache(forDocument: document, inSubKeys: [])
        
        try self.genCollection.diffs.insert(diff.makeDocument())
        try self.genCollection.states.update(matching: "_id" == self.stateIdentifier, to: self.makeDocument())
        
        return diff
    }
    
    public func constructState(atGeneration generation: Int32) throws -> Document {
        if generation == 0 || generation == lastDiff {
            return self.cachedObject
        }
        
        let cursor = try genCollection.diffs.find(matching: "tree" == self.treeIdentifier && "tree" == treeIdentifier && "generation" <= generation, sortedBy: ["diffId": .ascending], withBatchSize: generation)
        
        return try constructState(fromCursor: cursor)
    }
    
    private func constructState(fromCursor cursor: Cursor<Document>) throws -> Document {
        var doc: Document
        
        let diffCursor = Cursor(base: cursor, transform: {
            GenerationDiff(document: $0)
        })
        
        guard let initialDiff = diffCursor.next(), initialDiff.generation == 1 else {
            throw GenerationError.invalidDiffGeneration
        }
        
        var previousDiff = initialDiff.generation
        doc = initialDiff.diff
        
        for diff in diffCursor {
            guard diff.generation == previousDiff + 1 else {
                throw GenerationError.invalidDiffGeneration
            }
            
            previousDiff = diff.generation
            
            func updateCache(forDocument document: Document, inSubKeys subkeys: [SubscriptExpressionType]) {
                for (key, value) in document {
                    var subkeys = subkeys
                    subkeys.append(key)
                    
                    if let document = value as? Document, !document.validatesAsArray() {
                        updateCache(forDocument: document, inSubKeys: subkeys)
                    } else {
                        doc[raw: subkeys] = value
                    }
                }
            }
            
            updateCache(forDocument: diff.diff, inSubKeys: [])
        }
        
        return doc
    }
    
    public func constructState(atDate date: Date) throws -> Document {
        let cursor = try genCollection.diffs.find(matching: "tree" == self.treeIdentifier && "tree" == treeIdentifier && "creation" <= date, sortedBy: ["diffId": .ascending], withBatchSize: 100)
        
        return try constructState(fromCursor: cursor)
    }
    
    /// TODO: Get all changes to one or more field
    
    let genCollection: GenerationCollection
    
    let stateIdentifier: ObjectId
    let treeIdentifier: ObjectId
    public private(set) var cachedObject: Document
    public private(set) var currentGeneration: Int32 = 0
    public private(set) var lastDiff: Int32
    public private(set) var branch: Int32
    
    public func makeBSONPrimitive() -> BSONPrimitive {
        return makeDocument()
    }
    
    public func makeDocument() -> Document {
        return [
            "_id": stateIdentifier,
            "tree": treeIdentifier,
            "branch": branch,
            "object": cachedObject,
            "lastDiff": lastDiff,
            "generation": currentGeneration
        ]
    }
}
