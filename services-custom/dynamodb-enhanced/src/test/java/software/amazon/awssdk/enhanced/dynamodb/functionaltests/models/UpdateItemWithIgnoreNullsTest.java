/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.enhanced.dynamodb.functionaltests.models;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import software.amazon.awssdk.regions.Region;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.enhanced.dynamodb.mapper.testbeans.SimpleBean;

public class UpdateItemWithIgnoreNullsTest {

    public static final String TABLE = "UpdateItem";

    @Test // CS427 Issue link: https://github.com/aws/aws-sdk-java-v2/issues/2761
    public void success_if_ignoreNulls_is_true_and_any_attribute_is_null() throws InterruptedException {
        test_create_new_item_with_UpdateItem(true, null);
    }
    @Test // CS427 Issue link: https://github.com/aws/aws-sdk-java-v2/issues/2761
    public void failure_if_ignoreNulls_is_false_and_any_attribute_is_null() throws InterruptedException {
        test_create_new_item_with_UpdateItem(false, null);
    }
    @Test // CS427 Issue link: https://github.com/aws/aws-sdk-java-v2/issues/2761
    public void success_if_ignoreNulls_is_false_and_any_attribute_is_not_null() throws InterruptedException {
        test_create_new_item_with_UpdateItem(false, 123);
    }
    // // CS427 Issue link: https://github.com/aws/aws-sdk-java-v2/issues/2761
    private void test_create_new_item_with_UpdateItem(boolean ignoreNulls, Integer anyValue) throws InterruptedException {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
            "AKIA3JZTXEV4BASG2QGC",
            "apOg+/v1vm5gAHSFEW3lwAYeLszx7yOtp9gA0Tlz");
        Region region = Region.US_EAST_2;
        DynamoDbClient client =
            DynamoDbClient.builder().region(region).credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
        DynamoDbTable<SimpleBean> table = enhancedClient.table(TABLE, TableSchema.fromBean(SimpleBean.class));
        // delete
        TimeUnit.SECONDS.sleep(30);
        try {
            table.deleteTable();
        } catch (@SuppressWarnings("unused") ResourceNotFoundException ex) {
            // ignore
        }
        TimeUnit.SECONDS.sleep(30);
        table.createTable();
        TimeUnit.SECONDS.sleep(30);
        String id = "id";
        Key key = Key.builder().partitionValue(id).build();

        // read -> no item
        SimpleBean foundItem = table.getItem(b -> b.key(key).consistentRead(true));
        assertThat(foundItem).isNull();

        // update -> we expect a new item
        SimpleBean item = new SimpleBean();
        item.setId(id);
        if (anyValue != null) {
            item.setIntegerAttribute(anyValue);
        }
        SimpleBean updatedItem = table.updateItem(b -> b.item(item).ignoreNulls(ignoreNulls));

        // read -> we expect the created item
        foundItem = table.getItem(b -> b.key(key).consistentRead(true));
        assertThat(foundItem).withFailMessage("Item could not be found with GetItem after UpdateItem").isNotNull();
        assertThat(updatedItem).isNotNull();
    }
}