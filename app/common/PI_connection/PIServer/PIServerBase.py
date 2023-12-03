class PIServerBase:
    server = None

    def find_PI_point(self, tag_name: str):
        pass

    def find_PI_point_list(self, list_tag_name: list) -> list:
        pass

    def interpolated_of_tag_list(self, tag_list, time_range, span, numeric=False):
        pass

    def snapshot_of_tag_list(self, tag_list, time):
        pass
