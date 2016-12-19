import XCTest
@testable import Generations

let db = try! Database(mongoURL: "mongodb://localhost/generations")
let genCol = try! GenerationCollection(inDatabase: db)

class GenerationsTests: XCTestCase {
    func testExample() throws {
        var user: Document = [
            "username": "JoannisO",
            "password": "henk",
            "age": 20,
            "male": true,
            "contact": [
                "email": "joannis@orlandos.nl",
                "phone": "+00 000 000 00",
                "social": [
                    "skype": "joanniso"
                    ] as Document
                ] as Document
        ]
        
        let oldUser = user
        
        var state = try genCol.insertGeneration(fromDocument: user)
        
        let baseState = try state.constructState(atGeneration: 0)
        let firstState = try state.constructState(atGeneration: 1)
        
        XCTAssertEqual(user, baseState)
        XCTAssertEqual(user, firstState)
        
        user["username"] = "Joannis"
        user["contact", "email"] = "j.orlandos@autimatisering.nl"
        
        let diff = try state.updateState(updatingWith: [
                "username": "Joannis",
                "contact": [
                    "email": "j.orlandos@autimatisering.nl"
                ] as Document
            ])
        
        XCTAssertEqual(diff.diff, [
                "username": "Joannis",
                "contact": [
                    "email": "j.orlandos@autimatisering.nl"
                ] as Document
            ] as Document)
        
        let secondState = try state.constructState(atGeneration: diff.generation)
        
        XCTAssertEqual(user, secondState)
        XCTAssertEqual(user, secondState)
        
        guard let storedState = try genCol.findState(byId: state.stateIdentifier) else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(user, storedState.cachedObject)
        
        guard let storedState1 = try genCol.findPrimaryState(inTree: state.treeIdentifier) else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(user, storedState1.cachedObject)
        
        guard let storedState2 = try genCol.findState(forGeneration: 1, inTree: state.treeIdentifier) else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(oldUser, storedState2)
        
        _ = try state.updateState(updatingWith: [
                "password": "piet"
            ])
        
        guard let storedState3 = try genCol.findState(forGeneration: 2, inTree: state.treeIdentifier) else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(user, storedState3)
        
        user["password"] = "piet"
        
        guard let storedState4 = try genCol.findState(forGeneration: 3, inTree: state.treeIdentifier) else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(user, storedState4)
    }


    static var allTests : [(String, (GenerationsTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
