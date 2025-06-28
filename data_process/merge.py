import copy


def merge_listings(target_data, source_data):

    target_by_id = {listing["offer_id"]: listing for listing in target_data}

    # Merge source_data into target_data
    for item in source_data:
        if "offer_id" not in item and "offer_url" in item:
            for target_listing in target_by_id.values():
                if target_listing.get("offer_url") == item["offer_url"]:
                    item["offer_id"] = target_listing.get("offer_id")
                    item["is_unpublished"] = True
                    break

        if "offer_id" in item:
            offer_id = item["offer_id"]
            if offer_id in target_by_id:
                # Update existing item
                existing = target_by_id[offer_id]
                updated_date = item.get("updated_date")

                # Handle special cases if we have updated_date
                if updated_date:
                    # Case 1: Check if is_unpublished changed from false to true
                    if (
                        item.get("is_unpublished") is True
                        and existing.get("is_unpublished") is False
                    ):
                        existing["unpublished_date"] = updated_date

                    # Case 2: Check if offer_price changed
                    elif (
                        "price_value" in item
                        and "price_value" in existing
                        and isinstance(item["price_value"], (int, float))
                        and isinstance(existing["price_value"], (int, float))
                        and item["price_value"] != existing["price_value"]
                    ):
                        # Calculate price difference
                        price_diff = int(item["price_value"]) - int(
                            existing["price_value"]
                        )

                        # Update latest price change fields
                        existing["price_change_value"] = price_diff
                        existing["price_change_date"] = updated_date

                        # Update total price changes count
                        existing["total_price_changes"] = (
                            existing.get("total_price_changes", 0) + 1
                        )

                        # Append to price changes history
                        if existing.get("price_changes"):
                            existing["price_changes"] = (
                                f"{existing['price_changes']}, {price_diff}"
                            )
                        else:
                            existing["price_changes"] = str(price_diff)

                        if existing.get("price_changes_dates"):
                            existing["price_changes_dates"] = (
                                f"{existing['price_changes_dates']}, {updated_date}"
                            )
                        else:
                            existing["price_changes_dates"] = updated_date

                    # Always update last_active
                    existing["last_active"] = updated_date

                # Regular merge for all fields except updated_date
                for key, value in item.items():
                    # Skip updated_date - don't add it to merged data
                    if key == "updated_date":
                        continue

                    # Skip fields that shouldn't be updated
                    if key in ["timestamp", "image_urls"]:
                        continue

                    # Skip updating if new value is None
                    if value is None:
                        continue

                    # Skip updating description if offer is being unpublished
                    if key == "description" and item.get("is_unpublished", False):
                        continue

                    # For flattened structure, directly update values
                    existing[key] = value
            else:
                # New item
                new_item = copy.deepcopy(item)
                updated_date = new_item.get("updated_date")
                if updated_date:
                    # Assign values for new offer
                    new_item["publication_date"] = updated_date
                    new_item["last_active"] = updated_date

                    # Remove original field
                    new_item.pop("updated_date", None)

                target_by_id[offer_id] = new_item

    # Convert back to list
    return list(target_by_id.values())
