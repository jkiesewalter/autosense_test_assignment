from marshmallow import Schema, fields

class TransactionPayloadSchema(Schema):
    id = fields.Int(required=True)
    userId = fields.Int(required=True)
    chargerId = fields.Int(required=True)
    start = fields.DateTime(required=True)
    end = fields.DateTime(allow_none=True)
    kWhConsumed = fields.Float(required=True)
    status = fields.Str(required=True)
    paymentMethod = fields.Str(required=True)
    amount = fields.Float(required=True)
    currency = fields.Str(required=True)

class TransactionExtendedPayloadSchema(Schema):
    id = fields.Int(required=True)
    userId = fields.Int(required=True)
    chargerId = fields.Int(required=True)
    start = fields.DateTime(required=True)
    end = fields.DateTime(allow_none=True)
    kWhConsumed = fields.Float(required=True)
    status = fields.Str(required=True)
    paymentMethod = fields.Str(required=True)
    amount = fields.Float(required=True)
    currency = fields.Str(required=True)

class UserPayloadSchema(Schema):
    id = fields.Int(required=True)
    name = fields.Str(required=True)
    email = fields.Email(required=True)
    tier = fields.Str(required=True)
    createdAt = fields.DateTime(required=True)

class UserQueryParamsSchema(Schema):
    userId = fields.Int(required=False)
    firstName = fields.Str(required=False)
    lastName = fields.Str(required=False)
    email = fields.Str(required=False)

class ChargerPayloadSchema(Schema):
    id = fields.Int(required=True)
    city = fields.Str(required=True)
    location = fields.Str(required=True)
    installedAt = fields.DateTime(required=True)

class ChargerQueryParamsSchema(Schema):
    chargerId = fields.Int(required=False)
    city = fields.Str(required=False)

class ChargerUsageAnalyticsSchema(Schema):
    chargerId = fields.Int(required=True)
    totalTransactions = fields.Int(required=True)
    totalKWh = fields.Float(required=True)
    biggestTransactionKWh = fields.Float(required=True)
    smallestTransactionKWh = fields.Float(required=True)
    averageTransactionKWh = fields.Float(required=True)
    medianTransactionKWh = fields.Float(required=True)

class UsageAnalyticsQueryParamsSchema(Schema):
    startDatetime = fields.DateTime(required=False)
    endDatetime = fields.DateTime(required=False)
    status = fields.Str(required=False)

class TransactionQueryParamsSchema(Schema):
    minKwh = fields.Float(required=False)
    maxKwh = fields.Float(required=False)
    minAmountCharged = fields.Float(required=False)
    maxAmountCharged = fields.Float(required=False)
    userId = fields.Int(required=False)
    chargerId = fields.Int(required=False)
    startDatetime = fields.DateTime(required=False)
    endDatetime = fields.DateTime(required=False)