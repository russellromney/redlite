// coverage:ignore-file
// GENERATED CODE - DO NOT MODIFY BY HAND
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'api.dart';

// **************************************************************************
// FreezedGenerator
// **************************************************************************

T _$identity<T>(T value) => value;

final _privateConstructorUsedError = UnsupportedError(
    'It seems like you constructed your class using `MyClass._()`. This constructor is only meant to be used by freezed and you are not supposed to need it nor use it.\nPlease check the documentation here for more information: https://github.com/rrousselGit/freezed#adding-getters-and-methods-to-our-models');

/// @nodoc
mixin _$KeyInfo {
  KeyType get keyType => throw _privateConstructorUsedError;
  int get ttl => throw _privateConstructorUsedError;
  int get createdAt => throw _privateConstructorUsedError;
  int get updatedAt => throw _privateConstructorUsedError;

  /// Create a copy of KeyInfo
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $KeyInfoCopyWith<KeyInfo> get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $KeyInfoCopyWith<$Res> {
  factory $KeyInfoCopyWith(KeyInfo value, $Res Function(KeyInfo) then) =
      _$KeyInfoCopyWithImpl<$Res, KeyInfo>;
  @useResult
  $Res call({KeyType keyType, int ttl, int createdAt, int updatedAt});
}

/// @nodoc
class _$KeyInfoCopyWithImpl<$Res, $Val extends KeyInfo>
    implements $KeyInfoCopyWith<$Res> {
  _$KeyInfoCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of KeyInfo
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? keyType = null,
    Object? ttl = null,
    Object? createdAt = null,
    Object? updatedAt = null,
  }) {
    return _then(_value.copyWith(
      keyType: null == keyType
          ? _value.keyType
          : keyType // ignore: cast_nullable_to_non_nullable
              as KeyType,
      ttl: null == ttl
          ? _value.ttl
          : ttl // ignore: cast_nullable_to_non_nullable
              as int,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as int,
      updatedAt: null == updatedAt
          ? _value.updatedAt
          : updatedAt // ignore: cast_nullable_to_non_nullable
              as int,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$KeyInfoImplCopyWith<$Res> implements $KeyInfoCopyWith<$Res> {
  factory _$$KeyInfoImplCopyWith(
          _$KeyInfoImpl value, $Res Function(_$KeyInfoImpl) then) =
      __$$KeyInfoImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({KeyType keyType, int ttl, int createdAt, int updatedAt});
}

/// @nodoc
class __$$KeyInfoImplCopyWithImpl<$Res>
    extends _$KeyInfoCopyWithImpl<$Res, _$KeyInfoImpl>
    implements _$$KeyInfoImplCopyWith<$Res> {
  __$$KeyInfoImplCopyWithImpl(
      _$KeyInfoImpl _value, $Res Function(_$KeyInfoImpl) _then)
      : super(_value, _then);

  /// Create a copy of KeyInfo
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? keyType = null,
    Object? ttl = null,
    Object? createdAt = null,
    Object? updatedAt = null,
  }) {
    return _then(_$KeyInfoImpl(
      keyType: null == keyType
          ? _value.keyType
          : keyType // ignore: cast_nullable_to_non_nullable
              as KeyType,
      ttl: null == ttl
          ? _value.ttl
          : ttl // ignore: cast_nullable_to_non_nullable
              as int,
      createdAt: null == createdAt
          ? _value.createdAt
          : createdAt // ignore: cast_nullable_to_non_nullable
              as int,
      updatedAt: null == updatedAt
          ? _value.updatedAt
          : updatedAt // ignore: cast_nullable_to_non_nullable
              as int,
    ));
  }
}

/// @nodoc

class _$KeyInfoImpl implements _KeyInfo {
  const _$KeyInfoImpl(
      {required this.keyType,
      required this.ttl,
      required this.createdAt,
      required this.updatedAt});

  @override
  final KeyType keyType;
  @override
  final int ttl;
  @override
  final int createdAt;
  @override
  final int updatedAt;

  @override
  String toString() {
    return 'KeyInfo(keyType: $keyType, ttl: $ttl, createdAt: $createdAt, updatedAt: $updatedAt)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$KeyInfoImpl &&
            (identical(other.keyType, keyType) || other.keyType == keyType) &&
            (identical(other.ttl, ttl) || other.ttl == ttl) &&
            (identical(other.createdAt, createdAt) ||
                other.createdAt == createdAt) &&
            (identical(other.updatedAt, updatedAt) ||
                other.updatedAt == updatedAt));
  }

  @override
  int get hashCode =>
      Object.hash(runtimeType, keyType, ttl, createdAt, updatedAt);

  /// Create a copy of KeyInfo
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$KeyInfoImplCopyWith<_$KeyInfoImpl> get copyWith =>
      __$$KeyInfoImplCopyWithImpl<_$KeyInfoImpl>(this, _$identity);
}

abstract class _KeyInfo implements KeyInfo {
  const factory _KeyInfo(
      {required final KeyType keyType,
      required final int ttl,
      required final int createdAt,
      required final int updatedAt}) = _$KeyInfoImpl;

  @override
  KeyType get keyType;
  @override
  int get ttl;
  @override
  int get createdAt;
  @override
  int get updatedAt;

  /// Create a copy of KeyInfo
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$KeyInfoImplCopyWith<_$KeyInfoImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
mixin _$SetOptions {
  int? get ex => throw _privateConstructorUsedError;
  int? get px => throw _privateConstructorUsedError;
  bool get nx => throw _privateConstructorUsedError;
  bool get xx => throw _privateConstructorUsedError;

  /// Create a copy of SetOptions
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $SetOptionsCopyWith<SetOptions> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $SetOptionsCopyWith<$Res> {
  factory $SetOptionsCopyWith(
          SetOptions value, $Res Function(SetOptions) then) =
      _$SetOptionsCopyWithImpl<$Res, SetOptions>;
  @useResult
  $Res call({int? ex, int? px, bool nx, bool xx});
}

/// @nodoc
class _$SetOptionsCopyWithImpl<$Res, $Val extends SetOptions>
    implements $SetOptionsCopyWith<$Res> {
  _$SetOptionsCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of SetOptions
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? ex = freezed,
    Object? px = freezed,
    Object? nx = null,
    Object? xx = null,
  }) {
    return _then(_value.copyWith(
      ex: freezed == ex
          ? _value.ex
          : ex // ignore: cast_nullable_to_non_nullable
              as int?,
      px: freezed == px
          ? _value.px
          : px // ignore: cast_nullable_to_non_nullable
              as int?,
      nx: null == nx
          ? _value.nx
          : nx // ignore: cast_nullable_to_non_nullable
              as bool,
      xx: null == xx
          ? _value.xx
          : xx // ignore: cast_nullable_to_non_nullable
              as bool,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$SetOptionsImplCopyWith<$Res>
    implements $SetOptionsCopyWith<$Res> {
  factory _$$SetOptionsImplCopyWith(
          _$SetOptionsImpl value, $Res Function(_$SetOptionsImpl) then) =
      __$$SetOptionsImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({int? ex, int? px, bool nx, bool xx});
}

/// @nodoc
class __$$SetOptionsImplCopyWithImpl<$Res>
    extends _$SetOptionsCopyWithImpl<$Res, _$SetOptionsImpl>
    implements _$$SetOptionsImplCopyWith<$Res> {
  __$$SetOptionsImplCopyWithImpl(
      _$SetOptionsImpl _value, $Res Function(_$SetOptionsImpl) _then)
      : super(_value, _then);

  /// Create a copy of SetOptions
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? ex = freezed,
    Object? px = freezed,
    Object? nx = null,
    Object? xx = null,
  }) {
    return _then(_$SetOptionsImpl(
      ex: freezed == ex
          ? _value.ex
          : ex // ignore: cast_nullable_to_non_nullable
              as int?,
      px: freezed == px
          ? _value.px
          : px // ignore: cast_nullable_to_non_nullable
              as int?,
      nx: null == nx
          ? _value.nx
          : nx // ignore: cast_nullable_to_non_nullable
              as bool,
      xx: null == xx
          ? _value.xx
          : xx // ignore: cast_nullable_to_non_nullable
              as bool,
    ));
  }
}

/// @nodoc

class _$SetOptionsImpl extends _SetOptions {
  const _$SetOptionsImpl({this.ex, this.px, required this.nx, required this.xx})
      : super._();

  @override
  final int? ex;
  @override
  final int? px;
  @override
  final bool nx;
  @override
  final bool xx;

  @override
  String toString() {
    return 'SetOptions(ex: $ex, px: $px, nx: $nx, xx: $xx)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$SetOptionsImpl &&
            (identical(other.ex, ex) || other.ex == ex) &&
            (identical(other.px, px) || other.px == px) &&
            (identical(other.nx, nx) || other.nx == nx) &&
            (identical(other.xx, xx) || other.xx == xx));
  }

  @override
  int get hashCode => Object.hash(runtimeType, ex, px, nx, xx);

  /// Create a copy of SetOptions
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$SetOptionsImplCopyWith<_$SetOptionsImpl> get copyWith =>
      __$$SetOptionsImplCopyWithImpl<_$SetOptionsImpl>(this, _$identity);
}

abstract class _SetOptions extends SetOptions {
  const factory _SetOptions(
      {final int? ex,
      final int? px,
      required final bool nx,
      required final bool xx}) = _$SetOptionsImpl;
  const _SetOptions._() : super._();

  @override
  int? get ex;
  @override
  int? get px;
  @override
  bool get nx;
  @override
  bool get xx;

  /// Create a copy of SetOptions
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$SetOptionsImplCopyWith<_$SetOptionsImpl> get copyWith =>
      throw _privateConstructorUsedError;
}

/// @nodoc
mixin _$ZMember {
  double get score => throw _privateConstructorUsedError;
  Uint8List get member => throw _privateConstructorUsedError;

  /// Create a copy of ZMember
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  $ZMemberCopyWith<ZMember> get copyWith => throw _privateConstructorUsedError;
}

/// @nodoc
abstract class $ZMemberCopyWith<$Res> {
  factory $ZMemberCopyWith(ZMember value, $Res Function(ZMember) then) =
      _$ZMemberCopyWithImpl<$Res, ZMember>;
  @useResult
  $Res call({double score, Uint8List member});
}

/// @nodoc
class _$ZMemberCopyWithImpl<$Res, $Val extends ZMember>
    implements $ZMemberCopyWith<$Res> {
  _$ZMemberCopyWithImpl(this._value, this._then);

  // ignore: unused_field
  final $Val _value;
  // ignore: unused_field
  final $Res Function($Val) _then;

  /// Create a copy of ZMember
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? score = null,
    Object? member = null,
  }) {
    return _then(_value.copyWith(
      score: null == score
          ? _value.score
          : score // ignore: cast_nullable_to_non_nullable
              as double,
      member: null == member
          ? _value.member
          : member // ignore: cast_nullable_to_non_nullable
              as Uint8List,
    ) as $Val);
  }
}

/// @nodoc
abstract class _$$ZMemberImplCopyWith<$Res> implements $ZMemberCopyWith<$Res> {
  factory _$$ZMemberImplCopyWith(
          _$ZMemberImpl value, $Res Function(_$ZMemberImpl) then) =
      __$$ZMemberImplCopyWithImpl<$Res>;
  @override
  @useResult
  $Res call({double score, Uint8List member});
}

/// @nodoc
class __$$ZMemberImplCopyWithImpl<$Res>
    extends _$ZMemberCopyWithImpl<$Res, _$ZMemberImpl>
    implements _$$ZMemberImplCopyWith<$Res> {
  __$$ZMemberImplCopyWithImpl(
      _$ZMemberImpl _value, $Res Function(_$ZMemberImpl) _then)
      : super(_value, _then);

  /// Create a copy of ZMember
  /// with the given fields replaced by the non-null parameter values.
  @pragma('vm:prefer-inline')
  @override
  $Res call({
    Object? score = null,
    Object? member = null,
  }) {
    return _then(_$ZMemberImpl(
      score: null == score
          ? _value.score
          : score // ignore: cast_nullable_to_non_nullable
              as double,
      member: null == member
          ? _value.member
          : member // ignore: cast_nullable_to_non_nullable
              as Uint8List,
    ));
  }
}

/// @nodoc

class _$ZMemberImpl implements _ZMember {
  const _$ZMemberImpl({required this.score, required this.member});

  @override
  final double score;
  @override
  final Uint8List member;

  @override
  String toString() {
    return 'ZMember(score: $score, member: $member)';
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other.runtimeType == runtimeType &&
            other is _$ZMemberImpl &&
            (identical(other.score, score) || other.score == score) &&
            const DeepCollectionEquality().equals(other.member, member));
  }

  @override
  int get hashCode => Object.hash(
      runtimeType, score, const DeepCollectionEquality().hash(member));

  /// Create a copy of ZMember
  /// with the given fields replaced by the non-null parameter values.
  @JsonKey(includeFromJson: false, includeToJson: false)
  @override
  @pragma('vm:prefer-inline')
  _$$ZMemberImplCopyWith<_$ZMemberImpl> get copyWith =>
      __$$ZMemberImplCopyWithImpl<_$ZMemberImpl>(this, _$identity);
}

abstract class _ZMember implements ZMember {
  const factory _ZMember(
      {required final double score,
      required final Uint8List member}) = _$ZMemberImpl;

  @override
  double get score;
  @override
  Uint8List get member;

  /// Create a copy of ZMember
  /// with the given fields replaced by the non-null parameter values.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  _$$ZMemberImplCopyWith<_$ZMemberImpl> get copyWith =>
      throw _privateConstructorUsedError;
}
