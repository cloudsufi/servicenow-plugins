/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.servicenow.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

import static org.openqa.selenium.support.How.XPATH;

/**
 * ServiceNow batch source - Properties page - Locators.
 */
public class ServiceNowPropertiesPage {

  @FindBy(how = XPATH, using = "//div[contains(@class, 'label-input-container')]//input")
  public static WebElement labelInput;

  // Basic section
  @FindBy(how = XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement referenceNameInput;

  @FindBy(how = XPATH, using = "//div[@data-cy='select-queryMode']")
  public static WebElement modeDropdown;

  // Reporting Mode section
  @FindBy(how = XPATH, using = "//div[@data-cy='select-applicationName']")
  public static WebElement applicationNameDropdown;

  @FindBy(how = XPATH, using = "//input[@data-cy='tableNameField']")
  public static WebElement tableNameFieldInput;

  // Table Mode section
  @FindBy(how = XPATH, using = "//input[@data-cy='tableName']")
  public static WebElement tableNameInput;

  // Credentials section
  @FindBy(how = XPATH, using = "//input[@data-cy='clientId']")
  public static WebElement clientIdInput;

  @FindBy(how = XPATH, using = "//input[@data-cy='clientSecret']")
  public static WebElement clientSecretInput;

  @FindBy(how = XPATH, using = "//input[@data-cy='restApiEndpoint']")
  public static WebElement restApiEndpointInput;

  @FindBy(how = XPATH, using = "//input[@data-cy='user']")
  public static WebElement usernameInput;

  @FindBy(how = XPATH, using = "//input[@data-cy='password']")
  public static WebElement passwordInput;

  // Advanced section
  @FindBy(how = XPATH, using = "//div[@data-cy='select-valueType']")
  public static WebElement typeOfValuesDropdown;

  @FindBy(how = XPATH, using = "//div[@data-cy='schema-fields-list']" +
          "//*[local-name()='svg' and @data-cy='error-icon']")
  public static WebElement fieldNotCreatableError;

  // Connection Manager
  @FindBy(how = XPATH, using = "//input[@data-cy='toggle-NO']")
  public static WebElement verifyPluginPropertyToggleState;
  @FindBy(how = XPATH, using = "//*[contains(text(),'Add Connection')]")
  public static WebElement addConnectionButton;
  @FindBy(how = XPATH, using = "//div[@data-cy='connector-ServiceNow']")
  public static WebElement connectorServiceNow;
  @FindBy(how = XPATH, using = "//button[@data-cy='connection-test-button']")
  public static WebElement connectionTestButton;
  @FindBy(how = XPATH, using = "//button[@data-cy='connection-submit-button']")
  public static WebElement submitConnection;
  @FindBy(how = XPATH, using = "//*[contains(text(),'ServiceNowConnection')]")
  public static WebElement serviceNowConnectionButton;
  @FindBy(how = XPATH, using = "//button[@data-cy='connection']")
  public static WebElement connection;
}
