
class TransformInputs:
    def __init__(self):
        self.transform_mobile_number_column_indices = []
        self.transform_timestamp_column_indices = []

    def set_transform_mobile_column(self, mobile_number_column_indices: []):
        self.transform_mobile_number_column_indices = mobile_number_column_indices

    def set_transform_timestamp_column(self, timestamp_column_indices: []):
        self.transform_timestamp_column_indices = timestamp_column_indices

    def convert_transform_inputs(self, mobile_column_inputs: [], timestamp_column_inputs: []):
        if mobile_column_inputs and len(mobile_column_inputs) > 0:
            self.set_transform_mobile_column(mobile_number_column_indices=mobile_column_inputs)
        if timestamp_column_inputs and len(timestamp_column_inputs) > 0:
            self.set_transform_timestamp_column(timestamp_column_indices=timestamp_column_inputs)
