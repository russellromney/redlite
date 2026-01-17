/// Redlite - Redis API with SQLite durability.
///
/// A high-performance embedded database for Flutter/Dart that provides
/// Redis-compatible commands with SQLite's ACID durability.
///
/// ## Quick Start
///
/// ```dart
/// import 'package:redlite/redlite.dart';
///
/// void main() async {
///   // Initialize the library
///   await RustLib.init();
///
///   // Open an in-memory database
///   final db = RedliteDb.openMemory();
///
///   // Use Redis-like commands
///   await db.set('key', Uint8List.fromList('value'.codeUnits), null);
///   final value = await db.get('key');
///   print(utf8.decode(value!)); // 'value'
/// }
/// ```
library redlite;

export 'src/rust/api/mod.dart';
export 'src/rust/frb_generated.dart' show RustLib;
